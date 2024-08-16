/**
 * @since 1.0.0
 */
import type * as Rpc from "@effect/rpc/Rpc"
import type { NonEmptyArray } from "effect/Array"
import * as Arr from "effect/Array"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import { globalValue } from "effect/GlobalValue"
import * as Iterable from "effect/Iterable"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type { Predicate } from "effect/Predicate"
import * as Schema from "effect/Schema"
import type { Scope } from "effect/Scope"
import * as Envelope from "./Envelope.js"
import * as Message from "./Message.js"
import * as Reply from "./Reply.js"
import type { ShardId } from "./ShardId.js"
import type { ShardingConfig } from "./ShardingConfig.js"
import type { MessagePersistenceError } from "./ShardingError.js"
import { MalformedMessage } from "./ShardingError.js"
import * as Snowflake from "./Snowflake.js"

/**
 * @since 1.0.0
 * @category context
 */
export class MessageStorage extends Context.Tag("@effect/cluster/MessageStorage")<MessageStorage, {
  /**
   * Save the provided message and its associated metadata.
   */
  readonly saveRequest: <R extends Rpc.Any>(
    envelope: Message.OutgoingRequest<R>
  ) => Effect.Effect<SaveResult<R>, MessagePersistenceError | MalformedMessage>

  /**
   * Save the provided message and its associated metadata.
   */
  readonly saveEnvelope: (
    envelope: Message.OutgoingEnvelope
  ) => Effect.Effect<void, MessagePersistenceError | MalformedMessage>

  /**
   * Save the provided `Reply` and its associated metadata.
   */
  readonly saveReply: <R extends Rpc.Any>(
    reply: Reply.ReplyWithContext<R>
  ) => Effect.Effect<void, MessagePersistenceError | MalformedMessage>

  /**
   * Retrieves the replies received after the specified `Reply`.
   *
   * The MessageStorage implementation will suspend until replies are
   * available.
   */
  readonly repliesAfter: <R extends Rpc.Any>(
    lastReceivedReply: Reply.ReplyWithContext<R>,
    max: number
  ) => Effect.Effect<NonEmptyArray<Reply.Reply<R>>, MessagePersistenceError | MalformedMessage>

  /**
   * For locally sent messages, register a handler to process the replies.
   */
  readonly registerReplyHandler: <R extends Rpc.Any>(message: Message.OutgoingRequest<R>) => Effect.Effect<void>

  /**
   * Retrieves the unprocessed messages for the specified shards.
   *
   * The `sessionKey` parameter is used to ensure the same message is only ever
   * delivered once to the same session.
   */
  readonly unprocessedMessages: (
    shardIds: Iterable<ShardId>,
    sessionKey: object
  ) => Effect.Effect<Array<Message.Incoming<any>>, MessagePersistenceError>

  /**
   * Retrieves the unprocessed messages by id.
   */
  readonly unprocessedMessagesById: <R extends Rpc.Any>(
    messageIds: Iterable<Snowflake.Snowflake>
  ) => Effect.Effect<Array<Message.Incoming<R>>, MessagePersistenceError>
}>() {}

/**
 * @since 1.0.0
 * @category SaveResult
 */
export type SaveResult<R extends Rpc.Any> = SaveResult.Success | SaveResult.Duplicate<R>

/**
 * @since 1.0.0
 * @category SaveResult
 */
export const SaveResult = Data.taggedEnum<SaveResult.Constructor>()

/**
 * @since 1.0.0
 * @category SaveResult
 */
export const SaveResultEncoded = Data.taggedEnum<SaveResult.Encoded>()

/**
 * @since 1.0.0
 * @category SaveResult
 */
export declare namespace SaveResult {
  /**
   * @since 1.0.0
   * @category SaveResult
   */
  export type Encoded = SaveResult.Success | SaveResult.DuplicateEncoded

  /**
   * @since 1.0.0
   * @category SaveResult
   */
  export interface Success {
    readonly _tag: "Success"
  }

  /**
   * @since 1.0.0
   * @category SaveResult
   */
  export interface Duplicate<R extends Rpc.Any> {
    readonly _tag: "Duplicate"
    readonly originalId: Snowflake.Snowflake
    readonly lastReceivedReply: Option.Option<Reply.Reply<R>>
  }

  /**
   * @since 1.0.0
   * @category SaveResult
   */
  export interface DuplicateEncoded {
    readonly _tag: "Duplicate"
    readonly originalId: Snowflake.Snowflake
    readonly lastReceivedReply: Option.Option<Reply.ReplyEncoded<any>>
  }

  /**
   * @since 1.0.0
   * @category SaveResult
   */
  export interface Constructor extends Data.TaggedEnum.WithGenerics<1> {
    readonly taggedEnum: SaveResult<this["A"]>
  }
}

/**
 * @since 1.0.0
 * @category Encoded
 */
export type Encoded = {
  /**
   * Save the provided message and its associated metadata.
   */
  readonly saveEnvelope: (
    envelope: Envelope.Envelope.Encoded,
    primaryKey: string
  ) => Effect.Effect<SaveResult.Encoded, MessagePersistenceError>

  /**
   * Save the provided `Reply` and its associated metadata.
   */
  readonly saveReply: (
    reply: Reply.ReplyEncoded<any>
  ) => Effect.Effect<void, MessagePersistenceError>

  /**
   * Retrieves the replies received after the specified `Reply`.
   *
   * The MessageStorage implementation will suspend until replies are
   * available.
   */
  readonly repliesAfter: (
    lastReceivedReply: Reply.Reply<any>,
    max: number
  ) => Effect.Effect<NonEmptyArray<Reply.ReplyEncoded<any>>, MessagePersistenceError>

  /**
   * Retrieves the unprocessed messages for the given options.
   *
   * Unprocessed messages are:
   *
   * - For new shards, any requests without a "WithExit" reply.
   * - For new shards, any interrupts for unprocessed requests.
   * - For new shards, no "AckChunk" messages.
   *
   * - For existing shards, all messages without an "WithExit" reply after the
   *   cursor.
   */
  readonly unprocessedMessages: (
    options: EncodedUnprocessedOptions<any>
  ) => Effect.Effect<
    readonly [
      messages: Array<{
        readonly envelope: Envelope.Envelope.Encoded
        readonly lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
      }>,
      cursor: Option.Option<any>
    ],
    MessagePersistenceError
  >

  /**
   * Retrieves the unprocessed messages by id.
   */
  readonly unprocessedMessagesById: (
    messageIds: Iterable<Snowflake.Snowflake>
  ) => Effect.Effect<
    Array<{
      readonly envelope: Envelope.Envelope.Encoded
      readonly lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
    }>,
    MessagePersistenceError
  >
}

/**
 * @since 1.0.0
 * @category Encoded
 */
export type EncodedUnprocessedOptions<A> = {
  readonly existingShards: Array<number>
  readonly newShards: Array<number>
  readonly cursor: Option.Option<A>
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (
  storage: Omit<MessageStorage["Type"], "registerReplyHandler">
): Effect.Effect<MessageStorage["Type"]> =>
  Effect.sync(() => {
    const replyHandlers = new Map<Snowflake.Snowflake, (reply: Reply.Reply<any>) => Effect.Effect<void>>()
    return MessageStorage.of({
      ...storage,
      registerReplyHandler: (message) =>
        Effect.sync(() => {
          replyHandlers.set(message.envelope.requestId, message.respond)
        }),
      saveReply(reply) {
        return Effect.flatMap(storage.saveReply(reply), () => {
          const handler = replyHandlers.get(reply.reply.requestId)
          if (!handler) {
            return Effect.void
          } else if (reply.reply._tag === "WithExit") {
            replyHandlers.delete(reply.reply.requestId)
          }
          return handler(reply.reply)
        })
      }
    })
  })

/**
 * @since 1.0.0
 * @category constructors
 */
export const makeEncoded: (encoded: Encoded) => Effect.Effect<
  MessageStorage["Type"],
  never,
  Snowflake.Generator | Scope
> = Effect.fnUntraced(function*(encoded: Encoded) {
  const scope = yield* Effect.scope
  const snowflakeGen = yield* Snowflake.Generator
  const cursors = new WeakMap<object, {
    shardIds: Set<ShardId>
    cursor: Option.Option<any>
  }>()

  const storage: MessageStorage["Type"] = yield* make({
    saveRequest: (message) =>
      Message.serializeEnvelope(message).pipe(
        Effect.flatMap((envelope) => encoded.saveEnvelope(envelope, Envelope.primaryKey(message.envelope))),
        Effect.flatMap((result) => {
          if (result._tag === "Success" || result.lastReceivedReply._tag === "None") {
            return Effect.succeed(result as SaveResult<any>)
          }
          const duplicate = result
          const schema = Reply.Reply(message.rpc)
          return Schema.decode(schema)(result.lastReceivedReply.value).pipe(
            Effect.provide(message.context),
            MalformedMessage.refail,
            Effect.map((reply) =>
              SaveResult.Duplicate({
                originalId: duplicate.originalId,
                lastReceivedReply: Option.some(reply)
              })
            )
          )
        })
      ),
    saveEnvelope: (message) =>
      Message.serializeEnvelope(message).pipe(
        Effect.flatMap((envelope) => encoded.saveEnvelope(envelope, Envelope.primaryKey(message.envelope))),
        Effect.asVoid
      ),
    saveReply: (reply) => Effect.flatMap(Reply.serialize(reply), encoded.saveReply),
    repliesAfter(lastReceivedReply, max) {
      const schema = Schema.mutable(Schema.NonEmptyArray(Reply.Reply(lastReceivedReply.rpc)))
      return encoded.repliesAfter(lastReceivedReply.reply, max).pipe(
        Effect.flatMap((replies) =>
          Schema.decode(schema)(replies).pipe(
            Effect.provide(lastReceivedReply.context),
            MalformedMessage.refail
          )
        )
      )
    },
    unprocessedMessages: Effect.fnUntraced(function*(shardIds, sessionKey) {
      const meta = cursors.get(sessionKey)
      if (!meta) {
        const newShards = Array.from(shardIds)
        const [messages, cursor] = yield* encoded.unprocessedMessages({
          existingShards: [],
          newShards,
          cursor: Option.none()
        })
        const decoded = yield* decodeMessages(messages)
        cursors.set(sessionKey, { shardIds: new Set(newShards), cursor })
        return decoded
      }
      const nextShards = new Set<ShardId>()
      const existingShards = Arr.empty<ShardId>()
      const newShards = Arr.empty<ShardId>()
      for (const shardId of shardIds) {
        nextShards.add(shardId)
        if (meta.shardIds.has(shardId)) {
          existingShards.push(shardId)
        } else {
          newShards.push(shardId)
        }
      }
      const [messages, cursor] = yield* encoded.unprocessedMessages({
        existingShards,
        newShards,
        cursor: meta.cursor
      })
      const decoded = yield* decodeMessages(messages)
      meta.shardIds = nextShards
      meta.cursor = cursor
      return decoded
    }),
    unprocessedMessagesById(messageIds) {
      return Effect.flatMap(encoded.unprocessedMessagesById(messageIds), decodeMessages)
    }
  })

  const saveReply = (reply: Reply.ReplyWithContext<any>) =>
    Effect.catchTag(
      storage.saveReply(reply),
      "MessagePersistenceError",
      Effect.die
    )

  const decodeMessages = (
    envelopes: Array<{
      readonly envelope: Envelope.Envelope.Encoded
      readonly lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
    }>
  ) => {
    const messages: Array<Message.Incoming<any>> = []
    let index = 0

    // if we have a malformed message, we should not return it and update
    // the storage with a defect
    const decodeMessage = Effect.catchAll(
      Effect.suspend(() => decodeEnvelopeWithReply(envelopes[index])),
      (error) => {
        const envelope = envelopes[index]
        return storage.saveReply(Reply.ReplyWithContext.fromDefect({
          id: snowflakeGen.unsafeNext(),
          requestId: Snowflake.Snowflake(envelope.envelope.requestId),
          defect: error.toString()
        })).pipe(
          Effect.uninterruptible,
          Effect.forkIn(scope),
          Effect.asVoid
        )
      }
    )
    return Effect.as(
      Effect.whileLoop({
        while: () => index < envelopes.length,
        body: () => decodeMessage,
        step: (message) => {
          const envelope = envelopes[index++]
          if (!message) return
          messages.push(
            message.envelope._tag === "Request"
              ? new Message.IncomingRequest({
                envelope: message.envelope,
                lastSentReply: envelope.lastSentReply,
                respond: saveReply
              })
              : new Message.IncomingEnvelope({
                envelope: message.envelope
              })
          )
        }
      }),
      messages
    )
  }

  return storage
})

/**
 * @since 1.0.0
 * @category Constructors
 */
export const noop: MessageStorage["Type"] = globalValue(
  "@effect/cluster/MessageStorage/noop",
  () =>
    Effect.runSync(make({
      saveRequest: () => Effect.succeed(SaveResult.Success()),
      saveEnvelope: () => Effect.void,
      saveReply: () => Effect.void,
      repliesAfter: () => Effect.never,
      unprocessedMessages: () => Effect.succeed([]),
      unprocessedMessagesById: () => Effect.succeed([])
    }))
)

/**
 * @since 1.0.0
 * @category Memory
 */
export type MemoryEntry = {
  readonly envelope: Envelope.Request.Encoded
  lastReceivedReply: Option.Option<Reply.ReplyEncoded<any>>
  replies: Array<Reply.ReplyEncoded<any>>
}

/**
 * @since 1.0.0
 * @category Memory
 */
export class MemoryDriver extends Effect.Service<MemoryDriver>()("@effect/cluster/MessageStorage/MemoryDriver", {
  dependencies: [Snowflake.layerGenerator],
  scoped: Effect.gen(function*() {
    const requests = new Map<string, MemoryEntry>()
    const requestsByPrimaryKey = new Map<string, MemoryEntry>()
    const unprocessed = new Set<Envelope.Request.Encoded>()
    const replyIds = new Set<string>()

    const journal: Array<Envelope.Envelope.Encoded> = []

    const cursors = new WeakMap<{}, number>()

    const unprocessedWith = (predicate: Predicate<Envelope.Envelope.Encoded>) => {
      const messages: Array<{
        readonly envelope: Envelope.Envelope.Encoded
        readonly lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
      }> = []
      for (const envelope of unprocessed) {
        if (!predicate(envelope)) {
          continue
        }
        if (envelope._tag === "Request") {
          const entry = requests.get(envelope.requestId)
          messages.push({
            envelope,
            lastSentReply: Option.fromNullable(entry?.replies[entry.replies.length - 1])
          })
        } else {
          messages.push({
            envelope,
            lastSentReply: Option.none()
          })
        }
      }
      return messages
    }

    const replyLatch = yield* Effect.makeLatch()

    const encoded: Encoded = {
      saveEnvelope: (envelope, primaryKey) =>
        Effect.sync(() => {
          const existing = requestsByPrimaryKey.get(primaryKey)
          if (existing) {
            return SaveResultEncoded.Duplicate({
              originalId: Snowflake.Snowflake(existing.envelope.requestId),
              lastReceivedReply: existing.lastReceivedReply
            })
          }
          if (envelope._tag === "Request") {
            const entry: MemoryEntry = { envelope, replies: [], lastReceivedReply: Option.none() }
            requests.set(envelope.requestId, entry)
            requestsByPrimaryKey.set(primaryKey, entry)
            unprocessed.add(envelope)
          } else if (envelope._tag === "AckChunk") {
            const entry = requests.get(envelope.requestId)
            if (entry) {
              entry.lastReceivedReply = Arr.findFirst(entry.replies, (r) => r.id === envelope.replyId)
            }
          }
          journal.push(envelope)
          return SaveResultEncoded.Success()
        }),
      saveReply: (reply) =>
        Effect.sync(() => {
          const entry = requests.get(reply.requestId)
          if (!entry || replyIds.has(reply.id)) return
          if (reply._tag === "WithExit") {
            unprocessed.delete(entry.envelope)
          }
          entry.replies.push(reply)
          replyIds.add(reply.id)
          replyLatch.unsafeOpen()
        }),
      repliesAfter: (reply, n) =>
        Effect.suspend(function loop(): Effect.Effect<NonEmptyArray<Reply.ReplyEncoded<any>>> {
          const replyId = String(reply.id)
          const entry = requests.get(String(reply.requestId))
          if (!entry) {
            replyLatch.unsafeClose()
            return Effect.flatMap(replyLatch.await, loop)
          }
          const index = entry.replies.findIndex((r) => r.id === replyId)
          const replies = entry.replies.slice(index + 1, index + 1 + n)
          if (!Arr.isNonEmptyArray(replies)) {
            replyLatch.unsafeClose()
            return Effect.flatMap(replyLatch.await, loop)
          }
          return Effect.succeed(replies)
        }),
      unprocessedMessages: ({ cursor, existingShards, newShards }: EncodedUnprocessedOptions<number>) =>
        Effect.sync(() => {
          if (unprocessed.size === 0) return [[], cursor] as const
          const messages = Arr.empty<{
            envelope: Envelope.Envelope.Encoded
            lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
          }>()
          const existingCursor = Option.getOrElse(cursor, () => journal.indexOf(Iterable.unsafeHead(unprocessed)))
          const checkNew = newShards.length > 0
          let checkExisting = false
          let index = checkNew
            ? journal.indexOf(Iterable.unsafeHead(unprocessed))
            : existingCursor
          for (; index < journal.length; index++) {
            const message = journal[index]
            if (index === existingCursor) {
              checkExisting = true
            }
            if (checkNew && newShards.includes(message.address.shardId)) {
              switch (message._tag) {
                case "Request": {
                  if (!unprocessed.has(message)) {
                    break
                  }
                  const entry = requests.get(message.requestId)!
                  messages.push({
                    envelope: message,
                    lastSentReply: Arr.last(entry.replies)
                  })
                  break
                }
                case "Interrupt": {
                  messages.push({
                    envelope: message,
                    lastSentReply: Option.none()
                  })
                  break
                }
              }
            } else if (checkExisting && existingShards.includes(message.address.shardId)) {
              switch (message._tag) {
                case "Request": {
                  if (!unprocessed.has(message)) {
                    break
                  }
                  const entry = requests.get(message.requestId)!
                  messages.push({
                    envelope: message,
                    lastSentReply: Arr.last(entry.replies)
                  })
                  break
                }
                case "AckChunk":
                case "Interrupt": {
                  messages.push({
                    envelope: message,
                    lastSentReply: Option.none()
                  })
                  break
                }
              }
            }
          }
          return [messages, Option.some(index)] as const
        }),
      unprocessedMessagesById: (ids) =>
        Effect.sync(() => {
          const envelopeIds = new Set<string>()
          for (const id of ids) {
            envelopeIds.add(String(id))
          }
          return unprocessedWith((envelope) => envelopeIds.has(envelope.requestId))
        })
    }

    const storage = yield* makeEncoded(encoded)

    return {
      storage,
      encoded,
      requests,
      requestsByPrimaryKey,
      unprocessed,
      replyIds,
      journal,
      cursors
    } as const
  })
}) {}

/**
 * @since 1.0.0
 * @category layers
 */
export const layerNoop: Layer.Layer<MessageStorage> = Layer.succeed(MessageStorage, noop)

/**
 * @since 1.0.0
 * @category layers
 */
export const layerMemory: Layer.Layer<
  MessageStorage | MemoryDriver,
  never,
  ShardingConfig
> = Layer.scoped(MessageStorage, Effect.map(MemoryDriver, (_) => _.storage)).pipe(
  Layer.provideMerge(MemoryDriver.Default)
)

// --- internal ---

const EnvelopeWithReply: Schema.Schema<{
  readonly envelope: Envelope.Envelope.PartialEncoded
  readonly lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
}, {
  readonly envelope: Envelope.Envelope.Encoded
  readonly lastSentReply: Schema.OptionEncoded<Reply.ReplyEncoded<any>>
}> = Schema.Struct({
  envelope: Envelope.PartialEncoded,
  lastSentReply: Schema.OptionFromSelf(Reply.Encoded)
}) as any

const decodeEnvelopeWithReply = Schema.decode(EnvelopeWithReply)
