package io.airbyte.cdk.util

import com.google.common.collect.RangeSet

class ShardedRangeSet(
    private val nShards: Int,
    val unshardedRangeSet: RangeSet<Long>
) {

    fun addAll(rangeSet: ShardedRangeSet) {
        unshardedRangeSet.addAll(rangeSet.unshardedRangeSet)
    }
}
