describe.concurrent("THub", () => {
  describe.concurrent("concurrent publishers and subscribers", () => {
    it("one to one", async () => {
      const as = Chunk.range(1, 64)

      const tx = Effect.forEach(as, (n) =>
        Do(($) => {
          const deferred = $(Deferred.make<never, void>())
          const hub = $(THub.bounded<number>(n).commit)
          const subscriber = $(
            Effect.scoped(
              hub.subscribeScoped.flatMap((subscription) =>
                deferred.succeed(undefined as void) >
                  Effect.forEach(as.take(n), () => subscription.take.commit)
              )
            ).fork
          )

          $(deferred.await)
          $(Effect.forEach(as.take(n), (a) => hub.publish(a).commit).fork)

          const values = $(subscriber.join)

          return values == as.take(n)
        })).map(Chunk.$.forAll(identity))

      const result = await tx.unsafeRunPromise()

      assert.isTrue(result)
    })
    it("one to many", async () => {
      const as = Chunk.range(1, 64)

      const tx = Effect.forEach(as, (n) =>
        Do(($) => {
          const deferred1 = $(Deferred.make<never, void>())
          const deferred2 = $(Deferred.make<never, void>())
          const hub = $(THub.bounded<number>(n).commit)
          const subscriber1 = $(
            Effect.scoped(
              hub.subscribeScoped.flatMap((subscription) =>
                deferred1.succeed(undefined as void) >
                  Effect.forEach(as.take(n), () => subscription.take.commit)
              )
            ).fork
          )
          const subscriber2 = $(
            Effect.scoped(
              hub.subscribeScoped.flatMap((subscription) =>
                deferred2.succeed(undefined as void) >
                  Effect.forEach(as.take(n), () => subscription.take.commit)
              )
            ).fork
          )

          $(deferred1.await)
          $(deferred2.await)
          $(Effect.forEach(as.take(n), (a) => hub.publish(a).commit).fork)

          const values1 = $(subscriber1.join)
          const values2 = $(subscriber2.join)

          return values1 == as.take(n) && values2 == as.take(n)
        })).map(Chunk.$.forAll(identity))

      const result = await tx.unsafeRunPromise()

      assert.isTrue(result)
    })
    it("many to many", async () => {
      const as = Chunk.range(1, 64)

      const tx = Effect.forEach(as, (n) =>
        Do(($) => {
          const deferred1 = $(Deferred.make<never, void>())
          const deferred2 = $(Deferred.make<never, void>())
          const hub = $(THub.bounded<number>(n).commit)
          const subscriber1 = $(
            Effect.scoped(
              hub.subscribeScoped.flatMap((subscription) =>
                deferred1.succeed(undefined as void) >
                  Effect.forEach((as + as).take(n * 2), () => subscription.take.commit)
              )
            ).fork
          )
          const subscriber2 = $(
            Effect.scoped(
              hub.subscribeScoped.flatMap((subscription) =>
                deferred2.succeed(undefined as void) >
                  Effect.forEach((as + as).take(n * 2), () => subscription.take.commit)
              )
            ).fork
          )

          $(deferred1.await)
          $(deferred2.await)
          $(Effect.forEach(as.take(n), (a) => hub.publish(a).commit).fork)
          $(Effect.forEach(as.take(n).map((_) => -_), (a) => hub.publish(a).commit).fork)

          const values1 = $(subscriber1.join)
          const values2 = $(subscriber2.join)

          return values1.filter((_) => _ > 0) == as.take(n) && values1.filter((_) => _ < 0) == as.take(n).map((_) =>
                -_
              ) &&
            values2.filter((_) => _ > 0) == as.take(n) && values2.filter((_) => _ < 0) == as.take(n).map((_) => -_)
        })).map(Chunk.$.forAll(identity))

      const result = await tx.unsafeRunPromise()

      assert.isTrue(result)
    })
  })
})
