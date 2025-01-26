import * as Effect from "effect/Effect"

/** @internal */
export const withRun = <
  A extends {
    readonly run: (f: (...args: Array<any>) => Effect.Effect<void>) => Effect.Effect<never>
  }
>() =>
<EX, RX>(f: (write: Parameters<A["run"]>[0]) => Effect.Effect<Omit<A, "run">, EX, RX>): Effect.Effect<A, EX, RX> =>
  Effect.suspend(() => {
    const semaphore = Effect.unsafeMakeSemaphore(1)
    let buffer: Array<Array<any>> = []
    let write = (...args: Array<any>): Effect.Effect<void> => Effect.sync(() => buffer.push(args))
    return Effect.map(f((...args) => write(...args)), (a) => ({
      ...a,
      run(f) {
        return semaphore.withPermits(1)(Effect.gen(function*() {
          const prev = write
          write = f

          for (const value of buffer) {
            yield* write(...value)
          }
          buffer = []

          return yield* Effect.onExit(Effect.never, () => {
            write = prev
            return Effect.void
          })
        }))
      }
    } as A))
  })
