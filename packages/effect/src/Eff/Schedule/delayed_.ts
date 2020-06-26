import { Clock } from "../Clock"
import { succeedNow } from "../Effect/succeedNow"

import { delayedM_ } from "./delayedM_"
import { Schedule } from "./schedule"

/**
 * Returns a new schedule with the specified pure modification
 * applied to each delay produced by this schedule.
 */
export const delayed_ = <S, A, B, R = unknown>(
  self: Schedule<S, R & Clock, A, B>,
  f: (ms: number) => number
) => delayedM_(self, (x) => succeedNow(f(x)))
