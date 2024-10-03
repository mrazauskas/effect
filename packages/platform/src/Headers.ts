/**
 * @since 1.0.0
 */
import * as Schema from "@effect/schema/Schema"
import * as Equivalence from "effect/Equivalence"
import * as FiberRef from "effect/FiberRef"
import { dual, identity } from "effect/Function"
import { globalValue } from "effect/GlobalValue"
import type * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Record from "effect/Record"
import * as Redacted from "effect/Redacted"
import * as String from "effect/String"
import type { Mutable } from "effect/Types"

/**
 * @since 1.0.0
 * @category type ids
 */
export const HeadersTypeId: unique symbol = Symbol.for("@effect/platform/Headers")

/**
 * @since 1.0.0
 * @category type ids
 */
export type HeadersTypeId = typeof HeadersTypeId

/**
 * @since 1.0.0
 * @category refinements
 */
export const isHeaders = (u: unknown): u is Headers => Predicate.hasProperty(u, HeadersTypeId)

/**
 * @since 1.0.0
 * @category models
 */
export interface Headers {
  readonly [HeadersTypeId]: HeadersTypeId
  readonly [key: string]: HeaderValues
}

const Proto = Object.assign(Object.create(null), {
  [HeadersTypeId]: HeadersTypeId
})

const make = (input: Record.ReadonlyRecord<string, HeaderValues>): Mutable<Headers> =>
  Object.assign(Object.create(Proto), input) as Headers

const equiv = Redacted.getEquivalence(String.Equivalence)

/**
 * @since 1.0.0
 * @category schemas
 */
export const schemaFromSelf: Schema.Schema<Headers> = Schema.declare(isHeaders, {
  identifier: "Headers",
  equivalence: () =>
    Record.getEquivalence(
      Equivalence.make<string | Redacted.Redacted<string>>((self, that) =>
        Redacted.isRedacted(self)
          ? Redacted.isRedacted(that) && equiv(self, that)
          : String.isString(that) && String.Equivalence(self, that)
      )
    )
})

const HeaderValues = Schema.Union(Schema.String, Schema.Redacted(Schema.String))
type HeaderValues = typeof HeaderValues.Type

/**
 * @since 1.0.0
 * @category schemas
 */
export const schema: Schema.Schema<
  Headers,
  Record.ReadonlyRecord<
    string,
    string | ReadonlyArray<string>
  >
> = Schema
  .transform(
    Schema.Record({ key: Schema.String, value: Schema.Union(Schema.String, Schema.Array(Schema.String)) }),
    schemaFromSelf,
    {
      strict: true,
      decode: (_) => fromInput(_),
      encode: (_) => unredact(_)
    }
  )

/**
 * @since 1.0.0
 * @category models
 */
export type Input =
  | Record.ReadonlyRecord<string, HeaderValues | ReadonlyArray<HeaderValues> | undefined>
  | Iterable<readonly [string, HeaderValues]>

/**
 * @since 1.0.0
 * @category constructors
 */
export const empty: Headers = Object.create(Proto)

/**
 * @since 1.0.0
 * @category constructors
 */
export const fromInput: (input?: Input) => Headers = (input) => {
  if (input === undefined) {
    return empty
  } else if (Symbol.iterator in input) {
    const out: Record<string, HeaderValues> = Object.create(Proto)
    for (const [k, v] of input) {
      out[k.toLowerCase()] = v
    }
    return out as Headers
  }
  const out: Record<string, HeaderValues> = Object.create(Proto)
  for (const [k, v] of Object.entries(input)) {
    if (Array.isArray(v)) {
      const redacted = v.some((_) => Redacted.isRedacted(_))
      out[k.toLowerCase()] = redacted
        ? Redacted.make(v.map((_) => Redacted.isRedacted(_) ? Redacted.value(_) : _).join(", "))
        : v.join(", ")
    } else if (v !== undefined) {
      out[k.toLowerCase()] = v as string
    }
  }
  return out as Headers
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const unsafeFromRecord = (input: Record.ReadonlyRecord<string, string>): Headers =>
  Object.setPrototypeOf(input, Proto) as Headers

/**
 * @since 1.0.0
 * @category combinators
 */
export const has: {
  (key: string): (self: Headers) => boolean
  (self: Headers, key: string): boolean
} = dual<
  (key: string) => (self: Headers) => boolean,
  (self: Headers, key: string) => boolean
>(2, (self, key) => key.toLowerCase() in self)

/**
 * @since 1.0.0
 * @category combinators
 */
export const get: {
  (key: string): (self: Headers) => Option.Option<string>
  (self: Headers, key: string): Option.Option<string>
} = dual<
  (key: string) => (self: Headers) => Option.Option<string>,
  (self: Headers, key: string) => Option.Option<string>
>(2, (self, key) => Record.get(self as Record<string, string>, key.toLowerCase()))

/**
 * @since 1.0.0
 * @category combinators
 */
export const set: {
  (key: string, value: HeaderValues): (self: Headers) => Headers
  (self: Headers, key: string, value: HeaderValues): Headers
} = dual<
  (key: string, value: HeaderValues) => (self: Headers) => Headers,
  (self: Headers, key: string, value: HeaderValues) => Headers
>(3, (self, key, value) => {
  const out = make(self)
  out[key.toLowerCase()] = value
  return out
})

/**
 * @since 1.0.0
 * @category combinators
 */
export const setAll: {
  (headers: Input): (self: Headers) => Headers
  (self: Headers, headers: Input): Headers
} = dual<
  (headers: Input) => (self: Headers) => Headers,
  (self: Headers, headers: Input) => Headers
>(2, (self, headers) =>
  make({
    ...self,
    ...fromInput(headers)
  }))

/**
 * @since 1.0.0
 * @category combinators
 */
export const merge: {
  (headers: Headers): (self: Headers) => Headers
  (self: Headers, headers: Headers): Headers
} = dual<
  (headers: Headers) => (self: Headers) => Headers,
  (self: Headers, headers: Headers) => Headers
>(2, (self, headers) => {
  const out = make(self)
  Object.assign(out, headers)
  return out
})

/**
 * @since 1.0.0
 * @category combinators
 */
export const remove: {
  (key: string): (self: Headers) => Headers
  (self: Headers, key: string): Headers
} = dual<
  (key: string) => (self: Headers) => Headers,
  (self: Headers, key: string) => Headers
>(2, (self, key) => {
  const out = make(self)
  delete out[key.toLowerCase()]
  return out
})

export const unredactHeader: {
  (self: string | Redacted.Redacted<string>): string
  (self: string | Redacted.Redacted<string> | undefined): string | undefined
} = (self) => (self !== undefined ? Redacted.isRedacted(self) ? Redacted.value(self) : self : undefined) as any

/**
 * @since 1.0.0
 * @category combinators
 */
export const unredact = (self: Headers): Record<string, string> => {
  const out: Record<string, string> = {}
  for (const name in self) {
    out[name] = unredactHeader(self[name])
  }

  return out
}

/**
 * @since 1.0.0
 * @category combinators
 */
export const redact: {
  (
    key: string | RegExp | ReadonlyArray<string | RegExp>
  ): (self: Headers) => Record<string, string | Redacted.Redacted>
  (
    self: Headers,
    key: string | RegExp | ReadonlyArray<string | RegExp>
  ): Record<string, string | Redacted.Redacted>
} = dual(
  2,
  (
    self: Headers,
    key: string | RegExp | ReadonlyArray<string | RegExp>
  ): Record<string, string | Redacted.Redacted> => {
    const out: Record<string, string | Redacted.Redacted> = { ...self }
    const modify = (key: string | RegExp) => {
      if (typeof key === "string") {
        const k = key.toLowerCase()
        if (k in self) {
          out[k] = Redacted.isRedacted(self[k]) ? self[k] : Redacted.make(self[k])
        }
      } else {
        for (const name in self) {
          if (key.test(name)) {
            out[name] = Redacted.isRedacted(self[name]) ? self[name] : Redacted.make(self[name])
          }
        }
      }
    }
    if (Array.isArray(key)) {
      for (let i = 0; i < key.length; i++) {
        modify(key[i])
      }
    } else {
      modify(key as string | RegExp)
    }
    return out
  }
)

/**
 * @since 1.0.0
 * @category fiber refs
 */
export const currentRedactedNames = globalValue(
  "@effect/platform/Headers/currentRedactedNames",
  () =>
    FiberRef.unsafeMake<ReadonlyArray<string | RegExp>>([
      "authorization",
      "cookie",
      "set-cookie",
      "x-api-key"
    ])
)
