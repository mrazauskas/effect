/**
 * @since 1.0.0
 */
import * as Rpc from "@effect/rpc/Rpc"
import * as RpcClient from "@effect/rpc/RpcClient"
import * as RpcGroup from "@effect/rpc/RpcGroup"
import * as RpcSchema from "@effect/rpc/RpcSchema"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as FiberRef from "effect/FiberRef"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import type { Scope } from "effect/Scope"
import { Persisted } from "./ClusterSchema.js"
import * as Envelope from "./Envelope.js"
import * as Message from "./Message.js"
import * as MessageStorage from "./MessageStorage.js"
import type { PodAddress } from "./PodAddress.js"
import * as Reply from "./Reply.js"
import type { ShardingConfig } from "./ShardingConfig.js"
import { AlreadyProcessingMessage, EntityNotManagedByPod, MailboxFull, PodUnavailable } from "./ShardingError.js"
import * as Snowflake from "./Snowflake.js"

/**
 * @since 1.0.0
 * @category context
 */
export class Pods extends Context.Tag("@effect/cluster/Pods")<Pods, {
  /**
   * Checks if a pod is responsive.
   */
  readonly ping: (address: PodAddress) => Effect.Effect<void, PodUnavailable>

  /**
   * Send a message locally.
   *
   * This ensures that the message hits storage before being sent to the local
   * entity.
   */
  readonly sendLocal: <R extends Rpc.Any>(
    options: {
      readonly message: Message.Outgoing<R>
      readonly send: <Rpc extends Rpc.Any>(
        message: Message.IncomingLocal<Rpc>
      ) => Effect.Effect<void, EntityNotManagedByPod | MailboxFull | AlreadyProcessingMessage>
      readonly simulateRemoteSerialization: boolean
    }
  ) => Effect.Effect<void, EntityNotManagedByPod | MailboxFull | AlreadyProcessingMessage>

  /**
   * Send a message to a pod.
   */
  readonly send: <R extends Rpc.Any>(
    options: {
      readonly address: PodAddress
      readonly message: Message.Outgoing<R>
    }
  ) => Effect.Effect<
    void,
    EntityNotManagedByPod | PodUnavailable | MailboxFull | AlreadyProcessingMessage
  >

  /**
   * Notify a pod that a message is available, then read replies from storage.
   */
  readonly notify: <R extends Rpc.Any>(
    options: {
      readonly address: PodAddress
      readonly message: Message.Outgoing<R>
      readonly discard: boolean
    }
  ) => Effect.Effect<void>

  /**
   * Notify the current pod that a message is available, then read replies from
   * storage.
   *
   * This ensures that the message hits storage before being sent to the local
   * entity.
   */
  readonly notifyLocal: <R extends Rpc.Any>(
    options: {
      readonly message: Message.Outgoing<R>
      readonly notify: (options: Message.IncomingLocal<any>) => Effect.Effect<void, EntityNotManagedByPod>
      readonly discard: boolean
    }
  ) => Effect.Effect<void>
}>() {}

/**
 * @since 1.0.0
 * @category Constructors
 */
export const make: (options: Omit<Pods["Type"], "sendLocal" | "notifyLocal">) => Effect.Effect<
  Pods["Type"],
  never,
  MessageStorage.MessageStorage | Snowflake.Generator
> = Effect.fnUntraced(function*(options: Omit<Pods["Type"], "sendLocal" | "notifyLocal">) {
  const storage = yield* MessageStorage.MessageStorage
  const snowflakeGen = yield* Snowflake.Generator

  const storageReplyAcks = new Map<Snowflake.Snowflake, Effect.Latch>()
  const requestIdRewrites = new Map<Snowflake.Snowflake, Snowflake.Snowflake>()

  function notifyWith<E>(
    message: Message.Outgoing<any>,
    afterPersist: (message: Message.Outgoing<any>, isDuplicate: boolean) => Effect.Effect<void, E>
  ): Effect.Effect<void, E> {
    const rpc = message.rpc as any as Rpc.AnyWithProps
    const persisted = Context.get(rpc.annotations, Persisted)
    if (!persisted) {
      return Effect.dieMessage("Pods.notify only supports persisted messages")
    }

    if (message._tag === "OutgoingEnvelope") {
      const rewriteId = requestIdRewrites.get(message.envelope.requestId)
      const requestId = rewriteId ?? message.envelope.requestId
      const latch = storageReplyAcks.get(requestId)
      if (rewriteId) {
        message = new Message.OutgoingEnvelope({
          ...message,
          envelope: message.envelope.withRequestId(rewriteId)
        })
      }
      return storage.saveEnvelope(message).pipe(
        Effect.orDie,
        Effect.zipRight(
          latch ? Effect.zipRight(latch.open, afterPersist(message, false)) : afterPersist(message, false)
        )
      )
    }

    // For requests, after persisting the request, we need to check if the
    // request is a duplicate. If it is, we need to resume from the last
    // received reply.
    //
    // Otherwise, we notify the remote entity and then reply from storage.
    return Effect.flatMap(
      Effect.orDie(storage.saveRequest(message)),
      MessageStorage.SaveResult.$match({
        Success: () => afterPersist(message, false),
        Duplicate: ({ lastReceivedReply, originalId }) => {
          requestIdRewrites.set(message.envelope.requestId, originalId)
          return afterPersist(
            new Message.OutgoingRequest({
              ...message,
              lastReceivedReply,
              envelope: Envelope.makeRequest({
                ...message.envelope,
                requestId: originalId
              }),
              respond(reply) {
                if (reply._tag === "WithExit") {
                  requestIdRewrites.delete(message.envelope.requestId)
                }
                return message.respond(reply.withRequestId(message.envelope.requestId))
              }
            }),
            true
          )
        }
      })
    )
  }

  const replyFromStorage = Effect.fnUntraced(
    function*(message: Message.OutgoingRequest<any>) {
      let reply = new Reply.ReplyWithContext({
        reply: message.lastReceivedReply.pipe(
          Option.getOrElse(() => Reply.Chunk.emptyFrom(message.envelope.requestId))
        ),
        context: message.context,
        rpc: message.rpc
      })

      if (reply.reply._tag === "WithExit") {
        return yield* message.respond(reply.reply)
      }

      while (true) {
        const replies = yield* storage.repliesAfter(reply, 4)

        for (const reply of replies) {
          // we have reached the end
          if (reply._tag === "WithExit") {
            storageReplyAcks.delete(reply.requestId)
            return yield* message.respond(reply)
          }

          let latch = storageReplyAcks.get(reply.requestId)
          if (!latch) {
            latch = Effect.unsafeMakeLatch(false)
            storageReplyAcks.set(reply.requestId, latch)
          } else {
            latch.unsafeClose()
          }

          yield* message.respond(reply)

          // wait for ack
          yield* latch.await
        }

        // continue from the last received reply
        reply = new Reply.ReplyWithContext({
          reply: replies[replies.length - 1],
          context: message.context,
          rpc: message.rpc
        })
      }
    },
    (effect, message) =>
      Effect.ensuring(
        effect,
        Effect.sync(() => {
          storageReplyAcks.delete(message.envelope.requestId)
        })
      )
  )

  return Pods.of({
    ...options,
    sendLocal(options) {
      const message = options.message
      if (!options.simulateRemoteSerialization) {
        return options.send(Message.incomingLocalFromOutgoing(message))
      }
      return Message.serialize(message).pipe(
        Effect.flatMap((encoded) => Message.deserializeLocal(message, encoded)),
        Effect.flatMap(options.send),
        Effect.catchTag("MalformedMessage", (error) => {
          if (message._tag === "OutgoingEnvelope") {
            return Effect.die(error)
          }
          return Effect.orDie(message.respond(
            new Reply.WithExit({
              id: snowflakeGen.unsafeNext(),
              requestId: message.envelope.requestId,
              exit: Exit.die(error)
            })
          ))
        })
      )
    },
    notify(options_) {
      const { discard, message } = options_
      return notifyWith(message, (message) => {
        if (message._tag === "OutgoingEnvelope") {
          return options.notify(options_)
        }
        return discard ? options.notify(options_) : options.notify(options_).pipe(
          Effect.fork,
          Effect.andThen(Effect.orDie(replyFromStorage(message)))
        )
      })
    },
    notifyLocal(options) {
      return notifyWith(options.message, (message, duplicate) => {
        if (options.discard || message._tag === "OutgoingEnvelope") {
          return Effect.orDie(options.notify(Message.incomingLocalFromOutgoing(message)))
        } else if (!duplicate) {
          return storage.registerReplyHandler(message).pipe(
            Effect.andThen(Effect.orDie(options.notify(Message.incomingLocalFromOutgoing(message))))
          )
        }
        return Effect.orDie(options.notify(Message.incomingLocalFromOutgoing(message))).pipe(
          Effect.fork,
          Effect.andThen(Effect.orDie(replyFromStorage(message)))
        )
      })
    }
  })
})

/**
 * @since 1.0.0
 * @category No-op
 */
export const makeNoop: Effect.Effect<
  Pods["Type"],
  never,
  MessageStorage.MessageStorage | Snowflake.Generator
> = make({
  send: ({ message }) => Effect.fail(new EntityNotManagedByPod({ address: message.envelope.address })),
  notify: () => Effect.void,
  ping: () => Effect.void
})

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerNoop: Layer.Layer<
  Pods,
  never,
  ShardingConfig | MessageStorage.MessageStorage
> = Layer.effect(Pods, makeNoop).pipe(Layer.provide([Snowflake.layerGenerator]))

const rpcErrors: Schema.Union<[
  typeof EntityNotManagedByPod,
  typeof MailboxFull,
  typeof AlreadyProcessingMessage
]> = Schema.Union(EntityNotManagedByPod, MailboxFull, AlreadyProcessingMessage)

/**
 * @since 1.0.0
 * @category Rpcs
 */
export class PodsRpcs extends RpcGroup.make(
  Rpc.make("Ping"),
  Rpc.make("Notify", {
    payload: {
      envelope: Envelope.PartialEncoded
    },
    success: Schema.Void,
    error: EntityNotManagedByPod
  }),
  Rpc.make("Effect", {
    payload: {
      request: Envelope.PartialEncodedRequest
    },
    success: Schema.Object as Schema.Schema<Reply.ReplyEncoded<any>>,
    error: rpcErrors
  }),
  Rpc.make("Stream", {
    payload: {
      request: Envelope.PartialEncodedRequest
    },
    error: rpcErrors,
    success: Schema.Object as Schema.Schema<Reply.ReplyEncoded<any>>,
    stream: true
  }),
  Rpc.make("Envelope", {
    payload: { envelope: Schema.Union(Envelope.AckChunk, Envelope.Interrupt) },
    error: rpcErrors
  })
) {}

/**
 * @since 1.0.0
 * @category Rpcs
 */
export interface PodsRpcClient extends RpcClient.FromGroup<typeof PodsRpcs> {}

/**
 * @since 1.0.0
 * @category Rpcs
 */
export const makeRpcClient: Effect.Effect<
  PodsRpcClient,
  never,
  RpcClient.Protocol | Scope
> = RpcClient.make(PodsRpcs, { spanPrefix: "Pods", disableTracing: true })

/**
 * @since 1.0.0
 * @category constructors
 */
export const makeRpc: Effect.Effect<
  Pods["Type"],
  never,
  Scope | RpcClientProtocol | MessageStorage.MessageStorage | Snowflake.Generator
> = Effect.gen(function*() {
  const makeClientProtocol = yield* RpcClientProtocol
  const snowflakeGen = yield* Snowflake.Generator

  const clients = yield* RcMap.make({
    lookup: (address: PodAddress) =>
      Effect.flatMap(
        makeClientProtocol(address),
        (protocol) => Effect.provideService(makeRpcClient, RpcClient.Protocol, protocol)
      ),
    idleTimeToLive: "1 minute"
  })

  return yield* make({
    ping(address) {
      return RcMap.get(clients, address).pipe(
        Effect.flatMap((client) => client.Ping()),
        Effect.scoped,
        Effect.catchAllCause(() => Effect.fail(new PodUnavailable({ address })))
      )
    },
    send({ address, message }) {
      if (message._tag === "OutgoingEnvelope") {
        return RcMap.get(clients, address).pipe(
          Effect.flatMap((client) => client.Envelope({ envelope: message.envelope })),
          Effect.scoped,
          Effect.catchAllDefect(() => Effect.fail(new PodUnavailable({ address })))
        )
      }
      const rpc = message.rpc as any as Rpc.AnyWithProps
      const isStream = RpcSchema.isStreamSchema(rpc.successSchema)
      if (!isStream) {
        return Effect.matchEffect(Message.serializeRequest(message), {
          onSuccess: (request) =>
            RcMap.get(clients, address).pipe(
              Effect.flatMap((client) => client.Effect({ request })),
              Effect.flatMap((reply) =>
                Schema.decode(Reply.Reply(message.rpc))(reply).pipe(
                  Effect.locally(FiberRef.currentContext, message.context),
                  Effect.orDie
                )
              ),
              Effect.flatMap(message.respond),
              Effect.scoped,
              Effect.catchAllDefect(() => Effect.fail(new PodUnavailable({ address })))
            ),
          onFailure: (error) =>
            message.respond(
              new Reply.WithExit({
                id: snowflakeGen.unsafeNext(),
                requestId: message.envelope.requestId,
                exit: Exit.die(error)
              })
            )
        })
      }
      return Effect.matchEffect(Message.serializeRequest(message), {
        onSuccess: (request) =>
          RcMap.get(clients, address).pipe(
            Effect.flatMap((client) => client.Stream({ request }, { asMailbox: true })),
            Effect.flatMap((mailbox) => {
              const decode = Schema.decode(Reply.Reply(message.rpc))
              return mailbox.take.pipe(
                Effect.flatMap((reply) => Effect.orDie(decode(reply))),
                Effect.flatMap(message.respond),
                Effect.forever,
                Effect.locally(FiberRef.currentContext, message.context),
                Effect.catchIf(Cause.isNoSuchElementException, () => Effect.void),
                Effect.catchAllDefect(() => Effect.fail(new PodUnavailable({ address })))
              )
            }),
            Effect.scoped
          ),
        onFailure: (error) =>
          message.respond(
            new Reply.WithExit({
              id: snowflakeGen.unsafeNext(),
              requestId: message.envelope.requestId,
              exit: Exit.die(error)
            })
          )
      })
    },
    notify({ address, message }) {
      const envelope = message.envelope
      return RcMap.get(clients, address).pipe(
        Effect.flatMap((client) => client.Notify({ envelope })),
        Effect.scoped,
        Effect.ignore
      )
    }
  })
})

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerRpc: Layer.Layer<
  Pods,
  never,
  MessageStorage.MessageStorage | RpcClientProtocol | ShardingConfig
> = Layer.scoped(Pods, makeRpc).pipe(
  Layer.provide(Snowflake.layerGenerator)
)

/**
 * @since 1.0.0
 * @category Client
 */
export class RpcClientProtocol extends Context.Tag("@effect/cluster/Pods/RpcClientProtocol")<
  RpcClientProtocol,
  (address: PodAddress) => Effect.Effect<RpcClient.Protocol["Type"], never, Scope>
>() {}
