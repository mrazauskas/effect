/**
 * @since 1.0.0
 */
import * as SqlClient from "@effect/sql/SqlClient"
import type { Row } from "@effect/sql/SqlConnection"
import type { SqlError } from "@effect/sql/SqlError"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import type { DurationInput } from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Envelope from "./Envelope.js"
import * as MessageStorage from "./MessageStorage.js"
import { SaveResultEncoded } from "./MessageStorage.js"
import type * as Reply from "./Reply.js"
import type { ShardingConfig } from "./ShardingConfig.js"
import { MessagePersistenceError } from "./ShardingError.js"
import * as Snowflake from "./Snowflake.js"

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeSql = Effect.fnUntraced(function*(options?: {
  readonly prefix?: string | undefined
}) {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms()
  const prefix = options?.prefix ?? "cluster"
  const table = (name: string) => `${prefix}_${name}`

  const messagesTable = table("messages")

  yield* sql.onDialectOrElse({
    mssql: () =>
      sql`
        IF OBJECT_ID(N'${sql(messagesTable)}', N'U') IS NULL
        CREATE TABLE ${sql(messagesTable)} (
          sequence BIGINT IDENTITY(1,1) PRIMARY KEY,
          id BIGINT NOT NULL,
          message_id VARCHAR(255) NOT NULL,
          shard_id INT NOT NULL,
          entity_type VARCHAR(255) NOT NULL,
          entity_id VARCHAR(255) NOT NULL,
          kind INT NOT NULL,
          tag VARCHAR(255),
          payload TEXT,
          headers TEXT,
          trace_id VARCHAR(255),
          span_id VARCHAR(255),
          sampled BIT,
          processed BIT NOT NULL DEFAULT 0,
          request_id BIGINT,
          reply_id BIGINT,
          CONSTRAINT ${sql(messagesTable + "_id")} UNIQUE (message_id)
        )
      `,
    mysql: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${sql(messagesTable)} (
          sequence BIGINT AUTO_INCREMENT PRIMARY KEY,
          id BIGINT NOT NULL,
          message_id VARCHAR(255) NOT NULL,
          shard_id INT NOT NULL,
          entity_type VARCHAR(255) NOT NULL,
          entity_id VARCHAR(255) NOT NULL,
          kind INT NOT NULL,
          tag VARCHAR(255),
          payload TEXT,
          headers TEXT,
          trace_id VARCHAR(255),
          span_id VARCHAR(255),
          sampled BOOLEAN,
          processed BOOLEAN NOT NULL DEFAULT FALSE,
          request_id BIGINT,
          reply_id BIGINT,
          CONSTRAINT ${sql(messagesTable + "_id")} UNIQUE (message_id)
        )
      `,
    pg: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${sql(messagesTable)} (
          sequence BIGSERIAL PRIMARY KEY,
          id BIGINT NOT NULL,
          message_id VARCHAR(255) NOT NULL,
          shard_id INT NOT NULL,
          entity_type VARCHAR(255) NOT NULL,
          entity_id VARCHAR(255) NOT NULL,
          kind INT NOT NULL,
          tag VARCHAR(255),
          payload TEXT,
          headers TEXT,
          trace_id VARCHAR(255),
          span_id VARCHAR(255),
          sampled BOOLEAN,
          processed BOOLEAN NOT NULL DEFAULT FALSE,
          request_id BIGINT,
          reply_id BIGINT,
          CONSTRAINT ${sql(messagesTable + "_id")} UNIQUE (message_id)
        )
      `.pipe(Effect.ignore),
    orElse: () =>
      // sqlite
      sql`
        CREATE TABLE IF NOT EXISTS ${sql(messagesTable)} (
          sequence INTEGER PRIMARY KEY AUTOINCREMENT,
          id INTEGER NOT NULL,
          message_id TEXT NOT NULL,
          shard_id INTEGER NOT NULL,
          entity_type TEXT NOT NULL,
          entity_id TEXT NOT NULL,
          kind INTEGER NOT NULL,
          tag TEXT,
          payload TEXT,
          headers TEXT,
          trace_id TEXT,
          span_id TEXT,
          sampled BOOLEAN,
          processed BOOLEAN NOT NULL DEFAULT FALSE,
          request_id INTEGER,
          reply_id INTEGER,
          UNIQUE (message_id)
        )
      `
  })

  // Add message indexes optimized for the specific query patterns
  const shardLookupIndex = `${messagesTable}_shard_idx`
  const idLookupIndex = `${messagesTable}_id_idx`
  yield* sql.onDialectOrElse({
    mssql: () =>
      sql`
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = ${shardLookupIndex})
        CREATE INDEX ${sql(shardLookupIndex)} 
        ON ${sql(messagesTable)} (shard_id, kind, processed, sequence);

        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = ${idLookupIndex})
        CREATE INDEX ${sql(idLookupIndex)}
        ON ${sql(messagesTable)} (id, processed, sequence);
      `,
    mysql: () =>
      sql`
        CREATE INDEX ${sql(shardLookupIndex)}
        ON ${sql(messagesTable)} (shard_id, kind, processed, sequence);

        CREATE INDEX ${sql(idLookupIndex)}
        ON ${sql(messagesTable)} (id, processed, sequence);
      `.unprepared.pipe(Effect.ignore),
    pg: () =>
      sql`
        CREATE INDEX IF NOT EXISTS ${sql(shardLookupIndex)}
        ON ${sql(messagesTable)} (shard_id, kind, processed, sequence);

        CREATE INDEX IF NOT EXISTS ${sql(idLookupIndex)}
        ON ${sql(messagesTable)} (id, processed, sequence);
      `.unprepared.pipe(Effect.ignore),
    orElse: () =>
      // sqlite
      Effect.andThen(
        sql`
          CREATE INDEX IF NOT EXISTS ${sql(shardLookupIndex)}
          ON ${sql(messagesTable)} (shard_id, kind, processed, sequence)
        `,
        sql`
          CREATE INDEX IF NOT EXISTS ${sql(idLookupIndex)}
          ON ${sql(messagesTable)} (id, processed, sequence)
        `
      )
  })

  const repliesTable = table("replies")

  yield* sql.onDialectOrElse({
    mssql: () =>
      sql`
        IF OBJECT_ID(N'${sql(repliesTable)}', N'U') IS NULL
        CREATE TABLE ${sql(repliesTable)} (
          id BIGINT NOT NULL,
          kind INT NOT NULL,
          request_id BIGINT NOT NULL,
          payload TEXT NOT NULL,
          sequence INT
        )
      `,
    mysql: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${sql(repliesTable)} (
          id BIGINT PRIMARY KEY,
          kind INT NOT NULL,
          request_id BIGINT NOT NULL,
          payload TEXT NOT NULL,
          sequence INT
        )
      `,
    pg: () =>
      sql`
        CREATE TABLE IF NOT EXISTS ${sql(repliesTable)} (
          id BIGINT PRIMARY KEY,
          kind INT NOT NULL,
          request_id BIGINT NOT NULL,
          payload TEXT NOT NULL,
          sequence INT
        )
      `,
    orElse: () =>
      // sqlite
      sql`
        CREATE TABLE IF NOT EXISTS ${sql(repliesTable)} (
          id INTEGER PRIMARY KEY,
          kind INTEGER NOT NULL,
          request_id INTEGER NOT NULL,
          payload TEXT NOT NULL,
          sequence INTEGER
        )
      `
  })

  // Add reply indexes optimized for request_id lookups with id ordering
  const replyLookupIndex = `${repliesTable}_request_lookup_idx`
  yield* sql.onDialectOrElse({
    mssql: () =>
      sql`
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = ${replyLookupIndex})
        CREATE INDEX ${sql(replyLookupIndex)}
        ON ${sql(repliesTable)} (request_id, id);
      `,
    mysql: () =>
      sql`
        CREATE INDEX ${sql(replyLookupIndex)}
        ON ${sql(repliesTable)} (request_id, id);
      `.unprepared.pipe(Effect.ignore),
    pg: () =>
      sql`
        CREATE INDEX IF NOT EXISTS ${sql(replyLookupIndex)}
        ON ${sql(repliesTable)} (request_id, id);
      `,
    orElse: () =>
      // sqlite
      sql`
        CREATE INDEX IF NOT EXISTS ${sql(replyLookupIndex)}
        ON ${sql(repliesTable)} (request_id, id);
      `
  })

  const envelopeToRow = (envelope: Envelope.Envelope.Encoded, message_id: string): MessageRow => {
    switch (envelope._tag) {
      case "Request":
        return {
          id: envelope.requestId,
          message_id,
          shard_id: envelope.address.shardId,
          entity_type: envelope.address.entityType,
          entity_id: envelope.address.entityId,
          kind: messageKind.Request,
          tag: envelope.tag,
          payload: JSON.stringify(envelope.payload),
          headers: JSON.stringify(envelope.headers),
          trace_id: envelope.traceId,
          span_id: envelope.spanId,
          sampled: supportsBooleans ? envelope.sampled : envelope.sampled ? 1 : 0,
          request_id: null,
          reply_id: null
        }
      case "AckChunk":
        return {
          id: envelope.id,
          message_id,
          shard_id: envelope.address.shardId,
          entity_type: envelope.address.entityType,
          entity_id: envelope.address.entityId,
          kind: messageKind.AckChunk,
          tag: null,
          payload: null,
          headers: null,
          trace_id: null,
          span_id: null,
          sampled: null,
          request_id: envelope.requestId,
          reply_id: envelope.replyId
        }
      case "Interrupt":
        return {
          id: envelope.id,
          message_id,
          shard_id: envelope.address.shardId,
          entity_type: envelope.address.entityType,
          entity_id: envelope.address.entityId,
          kind: messageKind.Interrupt,
          payload: null,
          tag: null,
          headers: null,
          trace_id: null,
          span_id: null,
          sampled: null,
          request_id: envelope.requestId,
          reply_id: null
        }
    }
  }

  const replyToRow = (reply: Reply.ReplyEncoded<any>): ReplyRow => ({
    id: reply.id,
    kind: replyKind[reply._tag],
    request_id: reply.requestId,
    payload: reply._tag === "WithExit" ? JSON.stringify(reply.exit) : JSON.stringify(reply.values),
    sequence: reply._tag === "Chunk" ? reply.sequence : null
  })

  const supportsBooleans = sql.onDialectOrElse({
    mssql: () => false,
    sqlite: () => false,
    orElse: () => true
  })

  const messageFromRow = (row: MessageRow & ReplyJoinRow): {
    readonly envelope: Envelope.Envelope.Encoded
    readonly lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
  } => {
    switch (Number(row.kind) as 0 | 1 | 2) {
      case 0:
        return {
          envelope: {
            _tag: "Request",
            requestId: String(row.id),
            address: {
              shardId: Number(row.shard_id),
              entityType: row.entity_type,
              entityId: row.entity_id
            },
            tag: row.tag!,
            payload: JSON.parse(row.payload!),
            headers: JSON.parse(row.headers!),
            traceId: row.trace_id!,
            spanId: row.span_id!,
            sampled: !!row.sampled
          },
          lastSentReply: row.reply_id ?
            Option.some({
              _tag: "Chunk",
              id: String(row.reply_id),
              requestId: String(row.request_id),
              values: JSON.parse(row.reply_payload!)
            } as any) :
            Option.none()
        }
      case 1:
        return {
          envelope: {
            _tag: "AckChunk",
            id: String(row.id),
            requestId: String(row.request_id!),
            replyId: String(row.reply_id!),
            address: {
              shardId: Number(row.shard_id),
              entityType: row.entity_type,
              entityId: row.entity_id
            }
          },
          lastSentReply: Option.none()
        }
      case 2:
        return {
          envelope: {
            _tag: "Interrupt",
            id: String(row.id),
            requestId: String(row.request_id!),
            address: {
              shardId: Number(row.shard_id),
              entityType: row.entity_type,
              entityId: row.entity_id
            }
          },
          lastSentReply: Option.none()
        }
    }
  }

  const latestReplies = sql<ReplyRow>`
    SELECT id, kind, request_id, payload, sequence
    FROM ${sql(repliesTable)}
    WHERE id IN (
      SELECT MAX(id) as id
      FROM ${sql(repliesTable)}
      GROUP BY request_id
    )
  `
  const sqlFalse = sql.literal(supportsBooleans ? "FALSE" : "0")
  const sqlTrue = sql.literal(supportsBooleans ? "TRUE" : "1")

  return yield* MessageStorage.makeEncoded({
    saveEnvelope: (envelope, message_id) =>
      Effect.suspend(() => {
        const row = envelopeToRow(envelope, message_id)
        return sql.onDialectOrElse({
          pg: () =>
            sql`
              WITH inserted AS (
                INSERT INTO ${sql(messagesTable)} ${sql.insert(row)}
                ON CONFLICT (message_id) DO NOTHING
                RETURNING id
              ),
              existing AS (
                SELECT m.id, r.id as reply_id, r.kind as reply_kind, r.payload as reply_payload, r.sequence as reply_sequence
                FROM ${sql(messagesTable)} m
                LEFT JOIN ${sql(repliesTable)} r ON r.request_id = m.id
                WHERE m.message_id = ${message_id}
                AND NOT EXISTS (SELECT 1 FROM inserted)
                ORDER BY r.id DESC
                LIMIT 1
              )
              SELECT * FROM existing
            `,
          mysql: () =>
            sql`
              SELECT m.id, r.id as reply_id, r.kind as reply_kind, r.payload as reply_payload, r.sequence as reply_sequence
              FROM ${sql(messagesTable)} m
              LEFT JOIN ${sql(repliesTable)} r ON r.request_id = m.id
              WHERE m.message_id = ${message_id}
              ORDER BY r.id DESC
              LIMIT 1;
              INSERT INTO ${sql(messagesTable)} ${sql.insert(row)}
              ON DUPLICATE KEY UPDATE id = id;
`.unprepared.pipe(
                Effect.map(([rows]) => rows as any as ReadonlyArray<Row>)
              ),
          mssql: () =>
            sql`
              MERGE ${sql(messagesTable)} WITH (HOLDLOCK) AS target
              USING (SELECT ${message_id} as message_id) AS source
              ON target.message_id = source.message_id
              WHEN NOT MATCHED THEN
                INSERT ${sql.insert(row)}
              OUTPUT
                inserted.id,
                CASE
                  WHEN inserted.id IS NULL THEN (
                    SELECT TOP 1 r.id, r.kind, r.payload
                    FROM ${sql(repliesTable)} r
                    WHERE r.request_id = target.id
                    ORDER BY r.id DESC
                  )
                END as reply_id,
                CASE
                  WHEN inserted.id IS NULL THEN (
                    SELECT TOP 1 r.kind
                    FROM ${sql(repliesTable)} r
                    WHERE r.request_id = target.id
                    ORDER BY r.id DESC
                  )
                END as reply_kind,
                CASE
                  WHEN inserted.id IS NULL THEN (
                    SELECT TOP 1 r.payload
                    FROM ${sql(repliesTable)} r
                    WHERE r.request_id = target.id
                    ORDER BY r.id DESC
                  )
                END as reply_payload,
                CASE
                  WHEN inserted.id IS NULL THEN (
                    SELECT TOP 1 r.sequence
                    FROM ${sql(repliesTable)} r
                    WHERE r.request_id = target.id
                    ORDER BY r.id DESC
                  )
                END as reply_sequence;
              `,
          orElse: () =>
            sql`
              SELECT m.id, r.id as reply_id, r.kind as reply_kind, r.payload as reply_payload, r.sequence as reply_sequence
              FROM ${sql(messagesTable)} m
              LEFT JOIN ${sql(repliesTable)} r ON r.request_id = m.id
              WHERE m.message_id = ${message_id}
              ORDER BY r.id DESC
              LIMIT 1
`.pipe(
              Effect.tap(sql`INSERT OR IGNORE INTO ${sql(messagesTable)} ${sql.insert(row)}`),
              sql.withTransaction
            )
        }).pipe(
          Effect.map((rows) => {
            if (rows.length === 0) {
              return SaveResultEncoded.Success()
            }
            const row = rows[0]
            return SaveResultEncoded.Duplicate({
              originalId: Snowflake.Snowflake(row.id as any),
              lastReceivedReply: row.reply_id ?
                Option.some({
                  id: String(row.reply_id),
                  requestId: String(row.id),
                  _tag: row.reply_kind === replyKind.WithExit ? "WithExit" : "Chunk",
                  ...(row.reply_kind === replyKind.WithExit
                    ? { exit: JSON.parse(row.reply_payload as string) }
                    : {
                      sequence: Number(row.reply_sequence),
                      values: JSON.parse(row.reply_payload as string)
                    })
                } as any) :
                Option.none()
            })
          })
        )
      }).pipe(
        Effect.provideService(SqlClient.SafeIntegers, true),
        Effect.catchAllCause((cause) => Effect.fail(new MessagePersistenceError({ cause: Cause.squash(cause) })))
      ),

    saveReply: (reply) =>
      Effect.suspend(() => {
        const row = replyToRow(reply)
        const insert = sql`INSERT INTO ${sql(repliesTable)} ${sql.insert(row)}`
        return reply._tag === "WithExit" ?
          insert.pipe(
            Effect.andThen(sql`UPDATE ${sql(messagesTable)} SET processed = ${sqlTrue} WHERE id = ${reply.requestId}`),
            sql.withTransaction
          ) :
          insert
      }).pipe(
        Effect.asVoid,
        Effect.catchAllCause((cause) => Effect.fail(new MessagePersistenceError({ cause: Cause.squash(cause) })))
      ),

    repliesFor: (requestIds) =>
      // replies where:
      // - the request is in the list
      // - the kind is WithExit
      // - or the kind is Chunk and has not been acked yet
      sql<ReplyRow>`
        SELECT id, kind, request_id, payload, sequence
        FROM ${sql(repliesTable)}
        WHERE ${sql.in("request_id", requestIds)}
        AND (
          kind = ${sql.literal(String(replyKind.WithExit))}
          OR (
            kind = ${sql.literal(String(replyKind.Chunk))}
            AND id NOT IN (SELECT reply_id FROM ${sql(messagesTable)})
          )
        )
        ORDER BY id ASC
      `.pipe(
        Effect.provideService(SqlClient.SafeIntegers, true),
        Effect.map(Arr.map((row): Reply.ReplyEncoded<any> =>
          row.kind === replyKind.WithExit ?
            ({
              _tag: "WithExit",
              id: String(row.id),
              requestId: String(row.request_id),
              exit: JSON.parse(row.payload)
            }) :
            {
              _tag: "Chunk",
              id: String(row.id),
              requestId: String(row.request_id),
              values: JSON.parse(row.payload),
              sequence: Number(row.sequence!)
            }
        )),
        Effect.catchAllCause((cause) => Effect.fail(new MessagePersistenceError({ cause: Cause.squash(cause) })))
      ),

    unprocessedMessages: Effect.fnUntraced(
      function*(options) {
        const newShards = options.newShards
        const existingShards = options.existingShards
        const cursor = Option.getOrElse(options.cursor, () => 0)

        // For new shards: get unfinished requests and their interrupts
        let newShardsRows: ReadonlyArray<MessageJoinRow> | undefined
        if (newShards.length > 0) {
          newShardsRows = yield* sql<MessageJoinRow>`
            SELECT m.*, r.id as reply_id, r.kind as reply_kind, r.payload as reply_payload, r.sequence as reply_sequence
            FROM ${sql(messagesTable)} m
            LEFT JOIN (${latestReplies}) r ON r.request_id = m.id
            WHERE ${sql.in("m.shard_id", newShards)}
            AND (
              -- Get unprocessed requests
              (m.kind = ${sql.literal(String(messageKind.Request))} AND m.processed = ${sqlFalse})
              -- Get interrupts for unprocessed requests
              OR (m.kind = ${sql.literal(String(messageKind.Interrupt))} AND EXISTS (
                SELECT 1 FROM ${sql(messagesTable)} m2 
                WHERE m2.id = m.request_id AND m2.processed = ${sqlFalse}
              ))
            )
            ORDER BY m.sequence ASC
          `
        }

        // For existing shards: get all unprocessed messages
        let existingShardsRows: ReadonlyArray<MessageJoinRow> | undefined
        if (existingShards.length > 0 && Option.isSome(options.cursor)) {
          existingShardsRows = yield* sql<MessageJoinRow>`
            SELECT m.*, r.id as reply_id, r.kind as reply_kind, r.payload as reply_payload, r.sequence as reply_sequence
            FROM ${sql(messagesTable)} m
            LEFT JOIN (${latestReplies}) r ON r.request_id = m.id
            WHERE ${sql.in("m.shard_id", existingShards)}
            AND m.processed = ${sqlFalse}
            AND m.sequence > ${cursor}
            ORDER BY m.sequence ASC
          `
        }

        const messages = Arr.empty<{
          readonly envelope: Envelope.Envelope.Encoded
          readonly lastSentReply: Option.Option<Reply.ReplyEncoded<any>>
        }>()
        let maxSequence = cursor
        if (newShardsRows) {
          for (const row of newShardsRows) {
            const sequence = Number(row.sequence)
            messages.push(messageFromRow(row))
            if (sequence > maxSequence) {
              maxSequence = sequence
            }
          }
        }
        if (existingShardsRows) {
          for (const row of existingShardsRows) {
            const sequence = Number(row.sequence)
            messages.push(messageFromRow(row))
            if (sequence > maxSequence) {
              maxSequence = sequence
            }
          }
        }

        return [messages, Option.some(maxSequence)] as const
      },
      (effect, { existingShards, newShards }) =>
        existingShards.length && newShards.length ? sql.withTransaction(effect) : effect,
      Effect.provideService(SqlClient.SafeIntegers, true),
      Effect.catchAllCause((cause) => Effect.fail(new MessagePersistenceError({ cause: Cause.squash(cause) })))
    ),

    unprocessedMessagesById(ids) {
      const idArr = Array.from(ids, (id) => String(id))
      return sql<MessageRow & ReplyJoinRow>`
        SELECT m.*, r.id as reply_id, r.payload as reply_payload, r.sequence as reply_sequence
        FROM ${sql(messagesTable)} as m
        WHERE ${sql.in("m.id", idArr)} AND m.processed = 0
        LEFT JOIN (${latestReplies}) r ON r.request_id = m.id
      `.pipe(
        Effect.map(Arr.map(messageFromRow)),
        Effect.provideService(SqlClient.SafeIntegers, true),
        Effect.catchAllCause((cause) => Effect.fail(new MessagePersistenceError({ cause: Cause.squash(cause) })))
      )
    }
  })
})

/**
 * @since 1.0.0
 * @category Layers
 */
export const layer: Layer.Layer<
  MessageStorage.MessageStorage,
  SqlError,
  SqlClient.SqlClient | ShardingConfig
> = Layer.scoped(MessageStorage.MessageStorage, makeSql()).pipe(
  Layer.provide(Snowflake.layerGenerator)
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWith = (options: {
  readonly prefix?: string | undefined
  readonly replyPollInterval?: DurationInput | undefined
}): Layer.Layer<MessageStorage.MessageStorage, SqlError, SqlClient.SqlClient | ShardingConfig> =>
  Layer.scoped(MessageStorage.MessageStorage, makeSql(options)).pipe(
    Layer.provide(Snowflake.layerGenerator)
  )

// -------------------------------------------------------------------------------------------------
// internal
// -------------------------------------------------------------------------------------------------

const messageKind = {
  "Request": 0,
  "AckChunk": 1,
  "Interrupt": 2
} as const satisfies Record<Envelope.Envelope.Any["_tag"], number>

const replyKind = {
  "WithExit": 0,
  "Chunk": 1
} as const satisfies Record<Reply.Reply<any>["_tag"], number>

type MessageRow = {
  readonly id: string | bigint
  readonly message_id: string
  readonly shard_id: number | bigint
  readonly entity_type: string
  readonly entity_id: string
  readonly kind: 0 | 1 | 2 | 0n | 1n | 2n
  readonly tag: string | null
  readonly payload: string | null
  readonly headers: string | null
  readonly trace_id: string | null
  readonly span_id: string | null
  readonly sampled: boolean | number | bigint | null
  readonly request_id: string | bigint | null
  readonly reply_id: string | bigint | null
}

type ReplyRow = {
  readonly id: string | bigint
  readonly kind: 0 | 1 | 0n | 1n
  readonly request_id: string | bigint
  readonly payload: string
  readonly sequence: number | bigint | null
}

type ReplyJoinRow = {
  readonly reply_id: string | bigint | null
  readonly reply_payload: string | null
  readonly reply_sequence: number | bigint | null
}

type MessageJoinRow = MessageRow & ReplyJoinRow & {
  readonly sequence: number | bigint
}
