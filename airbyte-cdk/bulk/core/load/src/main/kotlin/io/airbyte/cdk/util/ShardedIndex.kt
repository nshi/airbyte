package io.airbyte.cdk.util

class ShardedIndex(
    val unshardedIndex: Long,
    val nShards: Int
) {
    val shardedIndex: Long
        get() = unshardedIndex / nShards.toLong()
}
