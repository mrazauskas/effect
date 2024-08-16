/**
 * @since 1.0.0
 */
import * as Context from "effect/Context"
import { constTrue } from "effect/Function"

/**
 * @since 1.0.0
 * @category Annotations
 */
export class Persisted extends Context.Reference<Persisted>()("@effect/cluster/ClusterSchema/Persisted", {
  defaultValue: constTrue
}) {}
