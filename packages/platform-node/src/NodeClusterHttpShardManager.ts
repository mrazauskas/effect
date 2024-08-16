/**
 * @since 1.0.0
 */
import * as HttpShardManager from "@effect/cluster/HttpShardManager"
import type * as PodsHealth from "@effect/cluster/PodsHealth"
import * as ShardingConfig from "@effect/cluster/ShardingConfig"
import * as ShardManager from "@effect/cluster/ShardManager"
import type { Storage } from "@effect/cluster/Storage"
import type * as Etag from "@effect/platform/Etag"
import type { HttpPlatform } from "@effect/platform/HttpPlatform"
import type { HttpServer } from "@effect/platform/HttpServer"
import type { ServeError } from "@effect/platform/HttpServerError"
import * as RpcSerialization from "@effect/rpc/RpcSerialization"
import type { ConfigError } from "effect/ConfigError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { createServer } from "node:http"
import type { NodeContext } from "./NodeContext.js"
import * as NodeHttpClient from "./NodeHttpClient.js"
import * as NodeHttpServer from "./NodeHttpServer.js"
import * as NodeSocket from "./NodeSocket.js"

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttpServer: Layer.Layer<
  | HttpPlatform
  | Etag.Generator
  | NodeContext
  | HttpServer,
  ServeError,
  ShardingConfig.ShardingConfig
> = Effect.gen(function*() {
  const config = yield* ShardingConfig.ShardingConfig
  return NodeHttpServer.layer(createServer, config.shardManagerAddress)
}).pipe(Layer.unwrapEffect)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttp: Layer.Layer<
  ShardManager.ShardManager,
  ServeError | ConfigError,
  | ShardingConfig.ShardingConfig
  | RpcSerialization.RpcSerialization
  | Storage
  | PodsHealth.PodsHealth
> = HttpShardManager.layerHttp.pipe(
  Layer.provide([layerHttpServer, NodeHttpClient.layerUndici, ShardManager.layerConfigFromEnv])
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWebsocket: Layer.Layer<
  ShardManager.ShardManager,
  ServeError | ConfigError,
  | ShardingConfig.ShardingConfig
  | RpcSerialization.RpcSerialization
  | Storage
  | PodsHealth.PodsHealth
> = HttpShardManager.layerWebsocket.pipe(
  Layer.provide([layerHttpServer, NodeSocket.layerWebSocketConstructor, ShardManager.layerConfigFromEnv])
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttpMsgPack: Layer.Layer<
  ShardManager.ShardManager,
  ServeError | ConfigError,
  Storage | PodsHealth.PodsHealth
> = Layer.provide(layerHttp, [
  RpcSerialization.layerMsgPack,
  ShardingConfig.layerFromEnv,
  ShardManager.layerConfigFromEnv
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttpNdjson: Layer.Layer<
  ShardManager.ShardManager,
  ServeError | ConfigError,
  Storage | PodsHealth.PodsHealth
> = Layer.provide(layerHttp, [
  RpcSerialization.layerNdjson,
  ShardingConfig.layerFromEnv,
  ShardManager.layerConfigFromEnv
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWebsocketMsgPack: Layer.Layer<
  ShardManager.ShardManager,
  ServeError | ConfigError,
  Storage | PodsHealth.PodsHealth
> = Layer.provide(layerWebsocket, [
  RpcSerialization.layerMsgPack,
  ShardingConfig.layerFromEnv,
  ShardManager.layerConfigFromEnv
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWebsocketJson: Layer.Layer<
  ShardManager.ShardManager,
  ServeError | ConfigError,
  Storage | PodsHealth.PodsHealth
> = Layer.provide(layerWebsocket, [
  RpcSerialization.layerJson,
  ShardingConfig.layerFromEnv,
  ShardManager.layerConfigFromEnv
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerPodsHealthHttpMsgPack: Layer.Layer<
  PodsHealth.PodsHealth,
  ConfigError
> = Layer.provide(HttpShardManager.layerPodsHealthHttp, [
  RpcSerialization.layerMsgPack,
  NodeHttpClient.layerUndici,
  ShardingConfig.layerFromEnv
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerPodsHealthHttpNdjson: Layer.Layer<
  PodsHealth.PodsHealth,
  ConfigError
> = Layer.provide(HttpShardManager.layerPodsHealthHttp, [
  RpcSerialization.layerNdjson,
  NodeHttpClient.layerUndici,
  ShardingConfig.layerFromEnv
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerPodsHealthWebsocketMsgPack: Layer.Layer<
  PodsHealth.PodsHealth,
  ConfigError
> = Layer.provide(HttpShardManager.layerPodsHealthWebsocket, [
  RpcSerialization.layerMsgPack,
  NodeSocket.layerWebSocketConstructor,
  ShardingConfig.layerFromEnv
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerPodsHealthWebsocketJson: Layer.Layer<
  PodsHealth.PodsHealth,
  ConfigError
> = Layer.provide(HttpShardManager.layerPodsHealthWebsocket, [
  RpcSerialization.layerJson,
  NodeSocket.layerWebSocketConstructor,
  ShardingConfig.layerFromEnv
])
