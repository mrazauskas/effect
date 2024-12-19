/**
 * @since 1.0.0
 */
import * as Context from "effect/Context"
import { dual, identity } from "effect/Function"
import * as Inspectable from "effect/Inspectable"
import type * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Record from "effect/Record"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
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
export interface Headers extends Inspectable.Redactable {
  readonly [HeadersTypeId]: HeadersTypeId
  readonly [key: string]: string
}

const Proto = Object.assign(Object.create(null), {
  [HeadersTypeId]: HeadersTypeId,
  [Inspectable.symbolRedactable](
    this: Headers
  ): Record<string, string | Redacted.Redacted<string>> {
    return redact(this, redactableContext.get(this))
  },
  [Inspectable.NodeInspectSymbol]() {
    return Inspectable.redact(this)
  }
})

const redactableContext = Inspectable.makeRedactableContext((context) => Context.get(context, RedactedNames))

const make = (input: Record.ReadonlyRecord<string, string>, options?: {
  readonly redactableContext?: Context.Context<never> | undefined
}): Mutable<Headers> => {
  const self = Object.assign(Object.create(Proto), input) as Headers
  redactableContext.register(self, options?.redactableContext, input)
  return self
}

/**
 * @since 1.0.0
 * @category schemas
 */
export const schemaFromSelf: Schema.Schema<Headers> = Schema.declare(isHeaders, {
  identifier: "Headers",
  equivalence: () => Record.getEquivalence(String.Equivalence)
})

/**
 * @since 1.0.0
 * @category schemas
 */
export const schema: Schema.Schema<Headers, Record.ReadonlyRecord<string, string | ReadonlyArray<string>>> = Schema
  .transform(
    Schema.Record({ key: Schema.String, value: Schema.Union(Schema.String, Schema.Array(Schema.String)) }),
    schemaFromSelf,
    { strict: true, decode: (record) => fromInput(record), encode: identity }
  )

/**
 * @since 1.0.0
 * @category models
 */
export type Input =
  | Record.ReadonlyRecord<string, string | ReadonlyArray<string> | undefined>
  | Iterable<readonly [string, string]>

/**
 * @since 1.0.0
 * @category constructors
 */
export const empty = (options?: {
  readonly redactableContext?: Context.Context<never> | undefined
}): Headers => make({}, options)

/**
 * @since 1.0.0
 * @category constructors
 */
export const fromInput = (input?: Input, options?: {
  readonly redactableContext?: Context.Context<never> | undefined
}): Headers => {
  if (input === undefined) {
    return empty(options)
  } else if (isHeaders(input)) {
    return input
  } else if (Symbol.iterator in input) {
    const out: Record<string, string> = make({}, options)
    for (const [k, v] of input) {
      out[k.toLowerCase()] = v
    }
    return out as Headers
  }
  const out: Record<string, string> = make({}, options)
  for (const [k, v] of Object.entries(input)) {
    if (Array.isArray(v)) {
      out[k.toLowerCase()] = v.join(", ")
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
export const unsafeFromRecord = (input: Record.ReadonlyRecord<string, string>, options?: {
  readonly redactableContext?: Context.Context<never> | undefined
}): Headers => {
  const self = Object.setPrototypeOf(input, Proto)
  redactableContext.register(self, options?.redactableContext, input)
  return self as Headers
}

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
  (key: string, value: string): (self: Headers) => Headers
  (self: Headers, key: string, value: string): Headers
} = dual<
  (key: string, value: string) => (self: Headers) => Headers,
  (self: Headers, key: string, value: string) => Headers
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
>(2, (self, headers) => {
  const out = make({
    ...self,
    ...fromInput(headers)
  })
  redactableContext.register(out, undefined, self)
  return out
})

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
  redactableContext.register(out, undefined, self)
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
          out[k] = Redacted.make(self[k])
        }
      } else {
        for (const name in self) {
          if (key.test(name)) {
            out[name] = Redacted.make(self[name])
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
 * @category references
 */
export class RedactedNames extends Context.Reference<RedactedNames>()<
  "@effect/platform/Headers/currentRedactedNames",
  ReadonlyArray<string | RegExp>
>("@effect/platform/Headers/currentRedactedNames", {
  defaultValue: () => [
    "authorization",
    "cookie",
    "set-cookie",
    "x-api-key"
  ]
}) {}
