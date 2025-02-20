import type * as Rpc from "@effect/rpc/Rpc"
import { RequestId } from "@effect/rpc/RpcMessage"
import * as RpcServer from "@effect/rpc/RpcServer"
import { Duration, FiberSet } from "effect"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import type { DurationInput } from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as FiberRef from "effect/FiberRef"
import { identity } from "effect/Function"
import * as HashMap from "effect/HashMap"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as ClusterMetrics from "../ClusterMetrics.js"
import { Persisted } from "../ClusterSchema.js"
import type { Entity, HandlersFrom } from "../Entity.js"
import { CurrentAddress, Request } from "../Entity.js"
import type { EntityAddress } from "../EntityAddress.js"
import type { EntityId } from "../EntityId.js"
import * as Envelope from "../Envelope.js"
import * as Message from "../Message.js"
import * as Reply from "../Reply.js"
import type { ShardId } from "../ShardId.js"
import { Sharding } from "../Sharding.js"
import { ShardingConfig } from "../ShardingConfig.js"
import { AlreadyProcessingMessage, EntityNotManagedByPod, MailboxFull, MalformedMessage } from "../ShardingError.js"
import * as Snowflake from "../Snowflake.js"
import { EntityReaper } from "./entityReaper.js"

/** @internal */
export interface EntityManager {
  readonly sendLocal: <R extends Rpc.Any>(
    message: Message.IncomingLocal<R>
  ) => Effect.Effect<void, EntityNotManagedByPod | MailboxFull | AlreadyProcessingMessage>

  readonly send: (
    message: Message.Incoming<any>
  ) => Effect.Effect<void, EntityNotManagedByPod | MailboxFull | AlreadyProcessingMessage>

  readonly isProcessingFor: (message: Message.IncomingEnvelope) => boolean

  readonly interruptShard: (shardId: ShardId) => Effect.Effect<void>
}

// Represents the entities managed by this entity manager
/** @internal */
export type EntityState = {
  readonly address: EntityAddress
  readonly mailboxGauge: Metric.Metric.Gauge<bigint>
  readonly write: RpcServer.RpcServer<any>["write"]
  readonly activeRequests: Map<bigint, {
    readonly rpc: Rpc.AnyWithProps
    readonly message: Message.IncomingRequestLocal<any>
    lastSentChunk: Option.Option<Reply.Chunk<Rpc.Any>>
    sequence: number
  }>
  lastActiveCheck: number
}

/** @internal */
export const make = Effect.fnUntraced(function*<
  Rpcs extends Rpc.Any,
  Handlers extends HandlersFrom<Rpcs>,
  RX
>(
  entity: Entity<Rpcs>,
  buildHandlers: Effect.Effect<Handlers, never, RX>,
  options: {
    readonly storageEnabled: boolean
    readonly maxIdleTime?: DurationInput | undefined
    readonly concurrency?: number | "unbounded" | undefined
    readonly mailboxCapacity?: number | "unbounded" | undefined
  }
) {
  const config = yield* ShardingConfig
  const sharding = yield* Sharding
  const snowflakeGen = yield* Snowflake.Generator
  const managerScope = yield* Effect.scope
  const mailboxCapacity = options.mailboxCapacity ?? config.entityMailboxCapacity
  const clock = yield* Effect.clock
  const context = yield* Effect.context<Rpc.Context<Rpcs> | Rpc.Middleware<Rpcs> | RX>()

  const activeServers = new Map<EntityId, EntityState>()
  const entities: RcMap.RcMap<
    EntityAddress,
    EntityState,
    EntityNotManagedByPod
  > = yield* RcMap.make({
    idleTimeToLive: Duration.infinity,
    lookup: Effect.fnUntraced(function*(address: EntityAddress) {
      if (yield* sharding.isShutdown) {
        return yield* new EntityNotManagedByPod({ address })
      }

      const scope = yield* Effect.scope
      const endLatch = yield* Effect.makeLatch()
      let isShuttingDown = false

      // Initiate the behavior for the entity
      const handlers = yield* entity.protocol.toHandlersContext(buildHandlers).pipe(
        Effect.provideService(CurrentAddress, address)
      )
      const server = yield* RpcServer.makeNoSerialization(entity.protocol, {
        spanPrefix: `${entity.type}(${address.entityId})`,
        concurrency: options.concurrency ?? 1,
        onFromServer(response): Effect.Effect<void> {
          switch (response._tag) {
            case "Exit": {
              const request = state.activeRequests.get(response.requestId)
              if (!request) return Effect.void
              state.activeRequests.delete(response.requestId)
              // For durable messages, ignore interrupts during shutdown.
              // They will be retried when the entity is restarted.
              if (
                options.storageEnabled &&
                isShuttingDown &&
                Context.get(request.rpc.annotations, Persisted) &&
                Exit.isInterrupted(response.exit)
              ) {
                return Effect.void
              }
              const reply = new Reply.WithExit({
                requestId: Snowflake.Snowflake(response.requestId),
                id: snowflakeGen.unsafeNext(),
                exit: response.exit
              })
              return Effect.orDie(request.message.respond(reply))
            }
            case "Chunk": {
              const request = state.activeRequests.get(response.requestId)
              if (!request) return Effect.void
              const reply = new Reply.Chunk({
                requestId: Snowflake.Snowflake(response.requestId),
                id: snowflakeGen.unsafeNext(),
                sequence: request.sequence,
                values: response.values
              })
              request.sequence++
              request.lastSentChunk = Option.some(reply)
              return Effect.orDie(request.message.respond(reply))
            }
            case "Defect": {
              const exit = Exit.die(response.defect)
              const requests = Array.from(state.activeRequests.values())
              state.activeRequests.clear()
              return Effect.forEach(
                requests,
                (entry) =>
                  Effect.ignore(entry.message.respond(
                    new Reply.WithExit({
                      id: snowflakeGen.unsafeNext(),
                      requestId: Snowflake.Snowflake(entry.message.envelope.requestId),
                      exit
                    })
                  )),
                { discard: true }
              ).pipe(
                Effect.forkIn(managerScope),
                Effect.andThen(RcMap.invalidate(entities, address)),
                Effect.andThen(Effect.annotateLogs(Effect.logError("Defect in entity", Cause.die(response.defect)), {
                  module: "EntityManager",
                  address
                }))
              )
            }
            case "ClientEnd": {
              return endLatch.open
            }
          }
        }
      }).pipe(Effect.provide(handlers))

      // During shutdown, signal that no more messages will be processed
      // and wait for the fiber to complete.
      //
      // If the termination timeout is reached, let the server clean itself up
      if (Duration.toMillis(config.entityTerminationTimeout) > 0) {
        yield* Scope.addFinalizer(
          scope,
          server.write(0, { _tag: "Eof" }).pipe(
            Effect.andThen(endLatch.await),
            Effect.timeoutOption(config.entityTerminationTimeout)
          )
        )
      }

      const state: EntityState = {
        address,
        mailboxGauge: ClusterMetrics.mailboxSize.pipe(
          Metric.tagged("type", entity.type),
          Metric.tagged("entityId", address.entityId)
        ),
        write: server.write,
        activeRequests: new Map(),
        lastActiveCheck: clock.unsafeCurrentTimeMillis()
      }

      // add servers to map for expiration check
      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => {
          isShuttingDown = true
          activeServers.delete(address.entityId)
        })
      )
      activeServers.set(address.entityId, state)

      return state
    }, Effect.locally(FiberRef.currentLogAnnotations, HashMap.empty()))
  })

  const reaper = yield* EntityReaper
  const maxIdleTime = Duration.toMillis(options.maxIdleTime ?? config.entityMaxIdleTime)
  if (Number.isFinite(maxIdleTime)) {
    yield* reaper.register({
      maxIdleTime,
      servers: activeServers,
      entities
    })
  }

  // update metrics for active servers
  const gauge = ClusterMetrics.entities.pipe(Metric.tagged("type", entity.type))
  yield* Effect.sync(() => {
    gauge.unsafeUpdate(BigInt(activeServers.size), [])
    for (const state of activeServers.values()) {
      state.mailboxGauge.unsafeUpdate(BigInt(state.activeRequests.size), [])
    }
  }).pipe(
    Effect.andThen(Effect.sleep(1000)),
    Effect.forever,
    Effect.forkIn(managerScope)
  )

  function sendLocal<R extends Rpc.Any>(
    message: Message.IncomingLocal<R>
  ): Effect.Effect<void, EntityNotManagedByPod | MailboxFull | AlreadyProcessingMessage> {
    return RcMap.get(entities, message.envelope.address).pipe(
      Effect.flatMap((server): Effect.Effect<void, EntityNotManagedByPod | MailboxFull | AlreadyProcessingMessage> => {
        switch (message._tag) {
          case "IncomingRequestLocal": {
            // If the request is already running, then we might have more than
            // one sender for the same request. In this case, the other senders
            // should resume from storage only.
            let entry = server.activeRequests.get(message.envelope.requestId)
            if (entry) {
              return Effect.fail(
                new AlreadyProcessingMessage({
                  envelopeId: message.envelope.requestId,
                  address: message.envelope.address
                })
              )
            }

            if (mailboxCapacity !== "unbounded" && server.activeRequests.size >= mailboxCapacity) {
              return Effect.fail(new MailboxFull({ address: message.envelope.address }))
            }

            entry = {
              rpc: entity.protocol.requests.get(message.envelope.tag)! as any as Rpc.AnyWithProps,
              message,
              lastSentChunk: message.lastSentReply as any,
              sequence: Option.match(message.lastSentReply, {
                onNone: () => 0,
                onSome: (reply) => reply._tag === "Chunk" ? reply.sequence + 1 : 0
              })
            }
            server.activeRequests.set(message.envelope.requestId, entry)
            return server.write(0, {
              ...message.envelope,
              id: message.envelope.requestId,
              payload: new Request({
                ...message.envelope,
                lastSentChunk: message.lastSentReply as any
              })
            } as any)
          }
          case "IncomingEnvelope": {
            const entry = server.activeRequests.get(message.envelope.requestId)
            if (!entry) {
              return Effect.fail(
                new EntityNotManagedByPod({
                  address: message.envelope.address
                })
              )
            } else if (
              message.envelope._tag === "AckChunk" &&
              Option.isSome(entry.lastSentChunk) &&
              message.envelope.replyId !== entry.lastSentChunk.value.id
            ) {
              return Effect.void
            }
            return server.write(
              0,
              message.envelope._tag === "AckChunk"
                ? { _tag: "Ack", requestId: RequestId(message.envelope.requestId) }
                : { _tag: "Interrupt", requestId: RequestId(message.envelope.requestId) }
            )
          }
        }
      }),
      Effect.scoped
    )
  }

  const interruptShard = Effect.fnUntraced(function*(shardId: ShardId) {
    while (true) {
      let fibers: FiberSet.FiberSet | undefined
      for (const state of activeServers.values()) {
        if (shardId !== state.address.shardId) {
          continue
        }
        fibers ??= yield* FiberSet.make()
        yield* RcMap.invalidate(entities, state.address).pipe(
          FiberSet.run(fibers)
        )
      }
      if (fibers) yield* FiberSet.awaitEmpty(fibers)
    }
  }, Effect.scoped)

  const decodeMessage = Schema.decode(makeMessageSchema(entity))

  return identity<EntityManager>({
    interruptShard,
    isProcessingFor(message) {
      const state = activeServers.get(message.envelope.address.entityId)
      if (!state) return false
      return state.activeRequests.has(message.envelope.requestId)
    },
    sendLocal,
    send: (message) =>
      decodeMessage(message).pipe(
        Effect.matchEffect({
          onFailure: (cause) => {
            if (message._tag === "IncomingEnvelope") {
              return Effect.die(new MalformedMessage({ cause }))
            }
            return Effect.orDie(message.respond(
              new Reply.ReplyWithContext({
                reply: new Reply.WithExit({
                  id: snowflakeGen.unsafeNext(),
                  requestId: message.envelope.requestId,
                  exit: Exit.die(new MalformedMessage({ cause }))
                }),
                rpc: entity.protocol.requests.get(message.envelope.tag)!,
                context
              })
            ))
          },
          onSuccess: (decoded) => {
            if (decoded._tag === "IncomingEnvelope") {
              return sendLocal(
                new Message.IncomingEnvelope(decoded)
              )
            }
            const request = message as Message.IncomingRequest<any>
            const rpc = entity.protocol.requests.get(decoded.envelope.tag)!
            return sendLocal(
              new Message.IncomingRequestLocal({
                envelope: decoded.envelope,
                lastSentReply: decoded.lastSentReply,
                respond: (reply) =>
                  request.respond(
                    new Reply.ReplyWithContext({
                      reply,
                      rpc,
                      context
                    })
                  )
              })
            )
          }
        }),
        Effect.provide(context as Context.Context<unknown>)
      )
  })
})

const makeMessageSchema = <Rpcs extends Rpc.Any>(entity: Entity<Rpcs>): Schema.Schema<
  {
    readonly _tag: "IncomingRequest"
    readonly envelope: Envelope.Request.Any
    readonly lastSentReply: Option.Option<Reply.Reply<Rpcs>>
  } | {
    readonly _tag: "IncomingEnvelope"
    readonly envelope: Envelope.AckChunk | Envelope.Interrupt
  },
  Message.Incoming<Rpcs>,
  Rpc.Context<Rpcs>
> => {
  const requests = Arr.empty<Schema.Schema.Any>()

  for (const rpc of entity.protocol.requests.values()) {
    requests.push(
      Schema.TaggedStruct("IncomingRequest", {
        envelope: Schema.transform(
          Schema.Struct({
            ...Envelope.PartialEncodedRequestFromSelf.fields,
            tag: Schema.Literal(rpc._tag),
            payload: (rpc as any as Rpc.AnyWithProps).payloadSchema
          }),
          Envelope.RequestFromSelf,
          {
            decode: (encoded) => Envelope.makeRequest(encoded),
            encode: identity
          }
        ),
        lastSentReply: Schema.OptionFromSelf(Reply.Reply(rpc))
      })
    )
  }

  return Schema.Union(
    ...requests,
    Schema.TaggedStruct("IncomingEnvelope", {
      envelope: Schema.Union(
        Schema.typeSchema(Envelope.AckChunk),
        Schema.typeSchema(Envelope.Interrupt)
      )
    })
  ) as any
}
