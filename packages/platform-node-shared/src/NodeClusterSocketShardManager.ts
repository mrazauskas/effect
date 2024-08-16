/**
 * @since 1.0.0
 */
import * as PodsHealth from "@effect/cluster/PodsHealth"
import * as ShardingConfig from "@effect/cluster/ShardingConfig"
import * as ShardManager from "@effect/cluster/ShardManager"
import * as SocketShardManager from "@effect/cluster/SocketShardManager"
import type { Storage } from "@effect/cluster/Storage"
import type * as SocketServer from "@effect/platform/SocketServer"
import * as RpcSerialization from "@effect/rpc/RpcSerialization"
import type { ConfigError } from "effect/ConfigError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
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
  return NodeSocketServer.layer(config.shardManagerAddress)
}).pipe(Layer.unwrapEffect)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layer: Layer.Layer<
  ShardManager.ShardManager,
  SocketServer.SocketServerError | ConfigError,
  | ShardingConfig.ShardingConfig
  | Storage
  | RpcSerialization.RpcSerialization
  | PodsHealth.PodsHealth
> = SocketShardManager.layer.pipe(
  Layer.provide([layerSocketServer, layerClientProtocol, ShardManager.layerConfigFromEnv])
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerMsgPack: Layer.Layer<
  ShardManager.ShardManager,
  SocketServer.SocketServerError | ConfigError,
  Storage | PodsHealth.PodsHealth
> = Layer.provide(layer, [RpcSerialization.layerMsgPack, ShardingConfig.layerFromEnv])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerNdjson: Layer.Layer<
  ShardManager.ShardManager,
  SocketServer.SocketServerError | ConfigError,
  Storage | PodsHealth.PodsHealth
> = Layer.provide(layer, [RpcSerialization.layerNdjson, ShardingConfig.layerFromEnv])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerPodsHealth: Layer.Layer<
  PodsHealth.PodsHealth,
  ConfigError,
  RpcSerialization.RpcSerialization
> = Layer.provide(PodsHealth.layerRpc, [layerClientProtocol, ShardingConfig.layerFromEnv])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerPodsHealthMsgPack: Layer.Layer<
  PodsHealth.PodsHealth,
  ConfigError
> = Layer.provide(layerPodsHealth, RpcSerialization.layerMsgPack)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerPodsHealthNdjson: Layer.Layer<
  PodsHealth.PodsHealth,
  ConfigError
> = Layer.provide(layerPodsHealth, RpcSerialization.layerNdjson)
