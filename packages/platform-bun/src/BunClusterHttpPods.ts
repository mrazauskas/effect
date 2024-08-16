/**
 * @since 1.0.0
 */
import * as HttpPods from "@effect/cluster/HttpPods"
import type * as MessageStorage from "@effect/cluster/MessageStorage"
import type * as Pods from "@effect/cluster/Pods"
import type { Sharding } from "@effect/cluster/Sharding"
import * as ShardingConfig from "@effect/cluster/ShardingConfig"
import type * as Etag from "@effect/platform/Etag"
import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import type { HttpPlatform } from "@effect/platform/HttpPlatform"
import type { HttpServer } from "@effect/platform/HttpServer"
import type { ServeError } from "@effect/platform/HttpServerError"
import * as RpcSerialization from "@effect/rpc/RpcSerialization"
import type { ConfigError } from "effect/ConfigError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
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
  if (Option.isNone(config.podAddress)) {
    return yield* Effect.dieMessage("BunClusterHttpPods.layerHttpServer: ShardingConfig.podAddress is None")
  }
  return BunHttpServer.layer(config.podAddress.value)
}).pipe(Layer.unwrapEffect)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttp: Layer.Layer<
  Sharding | Pods.Pods,
  ServeError,
  ShardingConfig.ShardingConfig | RpcSerialization.RpcSerialization | MessageStorage.MessageStorage
> = HttpPods.layerHttp.pipe(
  Layer.provide([layerHttpServer, FetchHttpClient.layer])
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWebsocket: Layer.Layer<
  Sharding | Pods.Pods,
  ServeError,
  ShardingConfig.ShardingConfig | RpcSerialization.RpcSerialization | MessageStorage.MessageStorage
> = HttpPods.layerWebsocket.pipe(
  Layer.provide([layerHttpServer, BunSocket.layerWebSocketConstructor])
)

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttpMsgPack: Layer.Layer<
  Sharding | Pods.Pods,
  ServeError | ConfigError,
  MessageStorage.MessageStorage
> = Layer.provide(layerHttp, [
  ShardingConfig.layerFromEnv,
  RpcSerialization.layerMsgPack
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerHttpNdjson: Layer.Layer<
  Sharding | Pods.Pods,
  ServeError | ConfigError,
  MessageStorage.MessageStorage
> = Layer.provide(layerHttp, [
  ShardingConfig.layerFromEnv,
  RpcSerialization.layerNdjson
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWebsocketMsgPack: Layer.Layer<
  Sharding | Pods.Pods,
  ServeError | ConfigError,
  MessageStorage.MessageStorage
> = Layer.provide(layerWebsocket, [
  ShardingConfig.layerFromEnv,
  RpcSerialization.layerMsgPack
])

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWebsocketJson: Layer.Layer<
  Sharding | Pods.Pods,
  ServeError | ConfigError,
  MessageStorage.MessageStorage
> = Layer.provide(layerWebsocket, [
  ShardingConfig.layerFromEnv,
  RpcSerialization.layerJson
])
