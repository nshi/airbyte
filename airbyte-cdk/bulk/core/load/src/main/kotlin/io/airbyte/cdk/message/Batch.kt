package io.airbyte.cdk.message

import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet
import io.airbyte.cdk.util.EmptyRange
import io.airbyte.cdk.util.ShardedRange
import io.airbyte.cdk.util.Indexes
import io.airbyte.cdk.util.ShardedRangeSet

/**
 * Represents an accumulated batch of records in some stage of processing.
 *
 * Emitted by the record accumulator per batch accumulated. Handled by
 * the batch processor, which may advanced the state and yield a new batch.
 */
open class Batch(
    val name: String? = null,
    val state: State = State.ACCUMULATING,
    val mergeWithNext: Boolean = false,
) {
    enum class State {
        ACCUMULATING,
        ACCUMULATED,
        PERSISTED,
        COMPLETE
    }

    fun withState(newState: State): Batch {
        return Batch(name, newState)
    }
}

data class BatchEnvelope(
    val batch: Batch,
    val ranges: Map<Int, ShardedRangeSet> = emptyMap(),
) {
    constructor(shard: Int, range: ShardedRange, batch: Batch):
        this(
            batch = batch,
            ranges = when (range) {
                is Indexes -> mapOf(shard to
                    ShardedRangeSet(range.nShards,
                        TreeRangeSet.create(listOf(range.unshardedRange))))
                is EmptyRange -> emptyMap()
            }
        )

    fun withBatch(newBatch: Batch): BatchEnvelope {
        return BatchEnvelope(newBatch, ranges)
    }
}
