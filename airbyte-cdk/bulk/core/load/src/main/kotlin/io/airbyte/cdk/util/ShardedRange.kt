package io.airbyte.cdk.util

import com.google.common.collect.Range

/**
 * Light wrapper around Guava's Range to allow for empty ranges.
 */
sealed class ShardedRange {
    fun withIndex(index: ShardedIndex): ShardedRange {
        return when (this) {
            is Indexes -> {
                val newRange = Range.closed(unshardedRange.lowerEndpoint(), index.unshardedIndex)
                Indexes(index.nShards, newRange)
            }
            is EmptyRange -> Indexes(index.nShards, Range.singleton(index.unshardedIndex))
        }
    }
}

data class Indexes(
    val nShards: Int,
    val unshardedRange: Range<Long>
): ShardedRange()
data object EmptyRange: ShardedRange()
