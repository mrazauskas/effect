/**
 * @since 1.0.0
 */
import * as RpcServer from "@effect/rpc/RpcServer"
import * as Effect from "effect/Effect"
import { constant } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Mailbox from "effect/Mailbox"
import * as Option from "effect/Option"
import * as Message from "./Message.js"
import type * as MessageStorage from "./MessageStorage.js"
import * as Pods from "./Pods.js"
import * as Reply from "./Reply.js"
import * as Sharding from "./Sharding.js"
import type { ShardingConfig } from "./ShardingConfig.js"
import * as ShardManager from "./ShardManager.js"
import * as SynchronizedClock from "./SynchronizedClock.js"

const constVoid = constant(Effect.void)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHandlers = Pods.PodsRpcs.toLayer(Effect.gen(function*() {
  const sharding = yield* Sharding.Sharding

  return {
    Ping: () => Effect.void,
    Notify: ({ envelope }) =>
      sharding.notify(
        envelope._tag === "Request"
          ? new Message.IncomingRequest({
            envelope,
            respond: constVoid,
            lastSentReply: Option.none()
          })
          : new Message.IncomingEnvelope({ envelope })
      ),
    Effect: ({ request }) => {
      let resume: (reply: Effect.Effect<Reply.ReplyEncoded<any>>) => void
      let replyEncoded: Reply.ReplyEncoded<any> | undefined
      const message = new Message.IncomingRequest({
        envelope: request,
        lastSentReply: Option.none(),
        respond(reply) {
          return Effect.flatMap(Reply.serialize(reply), (reply) => {
            if (resume) {
              resume(Effect.succeed(reply))
            } else {
              replyEncoded = reply
            }
            return Effect.void
          })
        }
      })
      return Effect.zipRight(
        sharding.send(message),
        Effect.async<Reply.ReplyEncoded<any>>((resume_) => {
          if (replyEncoded) {
            resume_(Effect.succeed(replyEncoded))
          } else {
            resume = resume_
          }
        })
      )
    },
    Stream: ({ request }) =>
      Effect.flatMap(
        Mailbox.make<Reply.ReplyEncoded<any>>(),
        (mailbox) =>
          Effect.as(
            sharding.send(
              new Message.IncomingRequest({
                envelope: request,
                lastSentReply: Option.none(),
                respond(reply) {
                  return Effect.flatMap(Reply.serialize(reply), (reply) => {
                    mailbox.unsafeOffer(reply)
                    return Effect.void
                  })
                }
              })
            ),
            mailbox
          )
      ),
    Envelope: ({ envelope }) => sharding.send(new Message.IncomingEnvelope({ envelope }))
  }
}))

/**
 * The `PodsServer` recieves messages from other pods and forwards them to the
 * `Sharding` layer.
 *
 * It also responds to `Ping` requests.
 *
 * @since 1.0.0
 * @category constructors
 */
export const layer: Layer.Layer<
  never,
  never,
  RpcServer.Protocol | Sharding.Sharding | MessageStorage.MessageStorage
> = RpcServer.layer(Pods.PodsRpcs, {
  spanPrefix: "PodsServer",
  disableSpanPropagation: true
}).pipe(Layer.provide(layerHandlers))

/**
 * A `PodsServer` layer that includes the `Pods` & `Sharding` clients.
 *
 * @since 1.0.0
 * @category constructors
 */
export const layerWithClients: Layer.Layer<
  Sharding.Sharding | Pods.Pods,
  never,
  | RpcServer.Protocol
  | ShardingConfig
  | Pods.RpcClientProtocol
  | MessageStorage.MessageStorage
> = layer.pipe(
  Layer.provideMerge(Sharding.layer),
  Layer.provideMerge(Pods.layerRpc),
  Layer.provideMerge(SynchronizedClock.layer),
  Layer.provide(ShardManager.layerClientRpc)
)
