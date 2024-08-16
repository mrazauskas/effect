import { Message, MessageStorage, ShardingConfig, Snowflake, SqlMessageStorage } from "@effect/cluster"
import { FileSystem } from "@effect/platform"
import { NodeFileSystem } from "@effect/platform-node"
import { Rpc } from "@effect/rpc"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { SqlClient } from "@effect/sql/SqlClient"
import { describe, expect, it } from "@effect/vitest"
import { Effect, Fiber, Layer, TestClock } from "effect"
import { MysqlContainer } from "../../sql-mysql2/test/utils.js"
import { PgContainer } from "../../sql-pg/test/utils.js"
import {
  makeChunkReply,
  makeEmptyReply,
  makeReply,
  makeRequest,
  PrimaryKeyTest,
  StreamRpc
} from "./MessageStorage.test.js"

const StorageLive = SqlMessageStorage.layer.pipe(
  Layer.provideMerge(Snowflake.layerGenerator),
  Layer.provide(ShardingConfig.layerDefaults)
)

const truncate = Effect.gen(function*() {
  const sql = yield* SqlClient
  yield* sql.onDialectOrElse({
    sqlite: () => sql`DELETE FROM cluster_messages`,
    orElse: () => sql`TRUNCATE TABLE cluster_messages`
  })
  yield* sql.onDialectOrElse({
    sqlite: () => sql`DELETE FROM cluster_replies`,
    orElse: () => sql`TRUNCATE TABLE cluster_replies`
  })
})

describe("SqlMessageStorage", () => {
  ;([
    ["pg", Layer.orDie(PgContainer.ClientLive)],
    ["mysql", Layer.orDie(MysqlContainer.ClientLive)],
    ["sqlite", Layer.orDie(SqliteLayer)]
  ] as const).forEach(([label, layer]) => {
    it.layer(StorageLive.pipe(Layer.provideMerge(layer)), {
      timeout: 30000
    })(label, (it) => {
      it.effect("saveRequest", () =>
        Effect.gen(function*() {
          const storage = yield* MessageStorage.MessageStorage
          const request = yield* makeRequest()
          let result = yield* storage.saveRequest(request)
          expect(result._tag).toEqual("Success")
          result = yield* storage.saveRequest(request)
          expect(result._tag).toEqual("Duplicate")

          const messages = yield* storage.unprocessedMessages([request.envelope.address.shardId], {})
          expect(messages).toHaveLength(1)
        }))

      it.effect("saveReply + saveRequest duplicate", () =>
        Effect.gen(function*() {
          const storage = yield* MessageStorage.MessageStorage
          const request = yield* makeRequest({
            rpc: StreamRpc
          })
          let result = yield* storage.saveRequest(request)
          expect(result._tag).toEqual("Success")

          yield* storage.saveReply(yield* makeChunkReply(request))

          result = yield* storage.saveRequest(request)
          expect(result._tag === "Duplicate" && result.lastReceivedReply._tag).toEqual("Some")
        }))

      it.effect("detects duplicates", () =>
        Effect.gen(function*() {
          const storage = yield* MessageStorage.MessageStorage
          yield* storage.saveRequest(
            yield* makeRequest({
              rpc: Rpc.fromTaggedRequest(PrimaryKeyTest),
              payload: new PrimaryKeyTest({ id: 123 })
            })
          )
          const result = yield* storage.saveRequest(
            yield* makeRequest({
              rpc: Rpc.fromTaggedRequest(PrimaryKeyTest),
              payload: new PrimaryKeyTest({ id: 123 })
            })
          )
          expect(result._tag).toEqual("Duplicate")
        }))

      it.effect("unprocessedMessages sessionKey", () =>
        Effect.gen(function*() {
          yield* truncate

          const storage = yield* MessageStorage.MessageStorage
          const request = yield* makeRequest()
          yield* storage.saveRequest(request)
          const sessionKey = {}
          let messages = yield* storage.unprocessedMessages([request.envelope.address.shardId], sessionKey)
          expect(messages).toHaveLength(1)
          messages = yield* storage.unprocessedMessages([request.envelope.address.shardId], sessionKey)
          expect(messages).toHaveLength(0)
          yield* storage.saveRequest(yield* makeRequest())
          messages = yield* storage.unprocessedMessages([request.envelope.address.shardId], sessionKey)
          expect(messages).toHaveLength(1)
        }))

      it.effect("unprocessedMessages excludes complete requests", () =>
        Effect.gen(function*() {
          yield* truncate

          const storage = yield* MessageStorage.MessageStorage
          const request = yield* makeRequest()
          yield* storage.saveRequest(request)
          yield* storage.saveReply(yield* makeReply(request))
          const sessionKey = {}
          const messages = yield* storage.unprocessedMessages([request.envelope.address.shardId], sessionKey)
          expect(messages).toHaveLength(0)
        }))

      it.effect("repliesAfter", () =>
        Effect.gen(function*() {
          yield* truncate

          yield* TestClock.setTime(Date.now())

          const storage = yield* MessageStorage.MessageStorage
          const request = yield* makeRequest()
          yield* storage.saveRequest(request)
          const fiber = yield* storage.repliesAfter(makeEmptyReply(request), 4).pipe(
            Effect.fork
          )
          yield* TestClock.adjust(5000)
          expect(fiber.unsafePoll()).toBeNull()
          yield* storage.saveReply(yield* makeReply(request))
          yield* TestClock.adjust(5000)
          const replies = yield* Fiber.join(fiber)
          expect(replies).toHaveLength(1)
          expect(replies[0].requestId).toEqual(request.envelope.requestId)
        }))

      it.effect("registerReplyHandler", () =>
        Effect.gen(function*() {
          const storage = yield* MessageStorage.MessageStorage
          const latch = yield* Effect.makeLatch()
          const request = yield* makeRequest()
          yield* storage.saveRequest(request)
          yield* storage.registerReplyHandler(
            new Message.OutgoingRequest({
              ...request,
              respond: () => latch.open
            })
          )
          yield* storage.saveReply(yield* makeReply(request))
          yield* latch.await
        }))
    })
  })
})

const SqliteLayer = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const dir = yield* fs.makeTempDirectoryScoped()
  return SqliteClient.layer({
    filename: dir + "/test.db"
  })
}).pipe(Layer.unwrapScoped, Layer.provide(NodeFileSystem.layer))
