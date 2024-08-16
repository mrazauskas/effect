import { Storage } from "@effect/cluster"
import { NodeClusterSocketShardManager, NodeRuntime } from "@effect/platform-node"
import { Layer } from "effect"

NodeClusterSocketShardManager.layerMsgPack.pipe(
  Layer.provide([Storage.layerMemory, NodeClusterSocketShardManager.layerPodsHealthMsgPack]),
  // Layer.provide(Logger.minimumLogLevel(LogLevel.All)),
  Layer.launch,
  NodeRuntime.runMain
)
