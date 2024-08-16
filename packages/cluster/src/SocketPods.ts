/**
 * @since 1.0.0
 */
import { SocketServer } from "@effect/platform/SocketServer"
import type * as RpcSerialization from "@effect/rpc/RpcSerialization"
import * as RpcServer from "@effect/rpc/RpcServer"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type { MessageStorage } from "./MessageStorage.js"
import type * as Pods from "./Pods.js"
import * as PodsServer from "./PodsServer.js"
import type * as Sharding from "./Sharding.js"
import type { ShardingConfig } from "./ShardingConfig.js"

const withLogAddress = <A, E, R>(layer: Layer.Layer<A, E, R>): Layer.Layer<A, E, R | SocketServer> =>
  Layer.effectDiscard(Effect.gen(function*() {
    const server = yield* SocketServer
    const address = server.address._tag === "UnixAddress"
      ? server.address.path
      : `${server.address.hostname}:${server.address.port}`
    yield* Effect.annotateLogs(Effect.logInfo(`Listening on: ${address}`), {
      package: "@effect/cluster",
      service: "Pods"
    })
  })).pipe(Layer.provideMerge(layer))

/**
 * @since 1.0.0
 * @category Layers
 */
export const layer: Layer.Layer<
  Sharding.Sharding | Pods.Pods,
  never,
  Pods.RpcClientProtocol | ShardingConfig | RpcSerialization.RpcSerialization | SocketServer | MessageStorage
> = PodsServer.layerWithClients.pipe(
  withLogAddress,
  Layer.provide(RpcServer.layerProtocolSocketServer)
)
