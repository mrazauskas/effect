/**
 * @since 1.0.0
 */
import type { MessageStorage } from "@effect/cluster/MessageStorage"
import type * as Pods from "@effect/cluster/Pods"
import type { Sharding } from "@effect/cluster/Sharding"
import * as ShardingConfig from "@effect/cluster/ShardingConfig"
import * as SocketPods from "@effect/cluster/SocketPods"
import type * as SocketServer from "@effect/platform/SocketServer"
import * as RpcSerialization from "@effect/rpc/RpcSerialization"
import type { ConfigError } from "effect/ConfigError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { layerClientProtocol } from "./NodeClusterSocketCommon.js"
import * as NodeSocketServer from "./NodeSocketServer.js"

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerSocketServer: Layer.Layer<
  SocketServer.SocketServer,
  SocketServer.SocketServerError,
  ShardingConfig.ShardingConfig
> = Effect.gen(function*() {
  const config = yield* ShardingConfig.ShardingConfig
  if (Option.isNone(config.podAddress)) {
    return yield* Effect.dieMessage("layerSocketServer: ShardingConfig.podAddress is None")
  }
  return NodeSocketServer.layer(config.podAddress.value)
}).pipe(Layer.unwrapEffect)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layer: Layer.Layer<
  Sharding | Pods.Pods,
  SocketServer.SocketServerError,
  RpcSerialization.RpcSerialization | ShardingConfig.ShardingConfig | MessageStorage
> = SocketPods.layer.pipe(
  Layer.provide([layerClientProtocol, layerSocketServer])
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerMsgPack: Layer.Layer<
  Sharding | Pods.Pods,
  SocketServer.SocketServerError | ConfigError,
  MessageStorage
> = layer.pipe(
  Layer.provide([ShardingConfig.layerFromEnv, RpcSerialization.layerMsgPack])
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerNdjson: Layer.Layer<
  Sharding | Pods.Pods,
  SocketServer.SocketServerError | ConfigError,
  MessageStorage
> = layer.pipe(
  Layer.provide([ShardingConfig.layerFromEnv, RpcSerialization.layerNdjson])
)
