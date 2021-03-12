// tracing: off

import { chain_ } from "./core"
import type { Effect } from "./effect"

/**
 * Like chain but ignores the input
 *
 * @dataFirst andThen_
 */
export function andThen<R1, E1, A1>(fb: Effect<R1, E1, A1>, __trace?: string) {
  return <R, E, A>(fa: Effect<R, E, A>) => andThen_(fa, fb, __trace)
}

/**
 * Like chain but ignores the input
 */
export function andThen_<R, E, A, R1, E1, A1>(
  fa: Effect<R, E, A>,
  fb: Effect<R1, E1, A1>,
  __trace?: string
) {
  return chain_(fa, () => fb, __trace)
}
