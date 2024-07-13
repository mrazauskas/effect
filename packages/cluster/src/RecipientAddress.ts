/**
 * @since 1.0.0
 */
import * as Schema from "@effect/schema/Schema"

const SymbolKey = "@effect/cluster/RecipientAddress"

/**
 * @since 1.0.0
 * @category type ids
 */
export const TypeId: unique symbol = Symbol.for(SymbolKey)

/**
 * @since 1.0.0
 * @category type ids
 */
export type TypeId = typeof TypeId

/**
 * A `RecipientAddress` represents the unique address of an entity which can
 * receive a message and perform some behaviour based on that message.
 *
 * The address is composed of the recipient type as well as an identifier for
 * the desired entity.
 *
 * @since 1.0.0
 * @category models
 */
export class RecipientAddress extends Schema.Class<RecipientAddress>(SymbolKey)({
  entityId: Schema.NonEmpty,
  recipientType: Schema.NonEmpty
}) {}

/**
 * @since 1.0.0
 */
export namespace RecipientAddress {
  /**
   * @since 1.0.0
   * @category models
   */
  export type Encoded = Schema.Schema.Encoded<typeof RecipientAddress>
}
