/**
 * @since 1.0.0
 */
import * as HttpShardManager from "@effect/cluster/HttpShardManager"
import type * as PodsHealth from "@effect/cluster/PodsHealth"
import * as ShardingConfig from "@effect/cluster/ShardingConfig"
import * as ShardManager from "@effect/cluster/ShardManager"
import type { Storage } from "@effect/cluster/Storage"
import type * as Etag from "@effect/platform/Etag"
import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import type { HttpPlatform } from "@effect/platform/HttpPlatform"
import type { HttpServer } from "@effect/platform/HttpServer"
import type { ServeError } from "@effect/platform/HttpServerError"
import * as RpcSerialization from "@effect/rpc/RpcSerialization"
import type { ConfigError } from "effect/ConfigError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type { BunContext } from "./BunContext.js"
import * as BunHttpServer from "./BunHttpServer.js"
import * as BunSocket from "./BunSocket.js"

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttpServer: Layer.Layer<
  | HttpPlatform
  | Etag.Generator
  | BunContext
  | HttpServer,
  ServeError,
  ShardingConfig.ShardingConfig
> = Effect.gen(function*() {
  const config = yield* ShardingConfig.ShardingConfig
  return BunHttpServer.layer(config.shardManagerAddress)
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
  Layer.provide([layerHttpServer, FetchHttpClient.layer, ShardManager.layerConfigFromEnv])
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
  Layer.provide([layerHttpServer, BunSocket.layerWebSocketConstructor, ShardManager.layerConfigFromEnv])
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
  FetchHttpClient.layer,
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
  FetchHttpClient.layer,
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
  BunSocket.layerWebSocketConstructor,
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
  BunSocket.layerWebSocketConstructor,
  ShardingConfig.layerFromEnv
])
