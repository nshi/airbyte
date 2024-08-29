package io.airbyte.cdk.message

import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet
import java.nio.file.Path

/**
 * Represents an accumulated batch of records in some stage of processing.
 *
 * Emitted by the record processor to describe the batch accumulated. Handled by
 * the batch processor, which may advance the state and yield a new batch.
 */
interface Batch {
    enum class State {
        LOCAL,
        PERSISTED,
        COMPLETE
    }

    val state: State

    /* TODO: Allow the implementor to return a DestinationState
        update to be persisted to the platform before advancing */
}

/**
 * Simple batch: use if you need no other metadata for processing.
 */
data class SimpleBatch(
    override val state: Batch.State
): Batch

/**
 * Represents a file of records locally staged.
 */
interface LocalStagedFile: Batch {
    val localPath: Path
    val totalSizeBytes: Long
}

/**
 * Represents a file of raw records staged to disk for
 * pre-processing. Used internally by the framework
 */
data class StagedRawMessagesFile(
    override val localPath: Path,
    override val totalSizeBytes: Long,
    override val state: Batch.State = Batch.State.LOCAL
): LocalStagedFile

/**
 * Internally-used wrapper for tracking the association
 * between a batch and the range of records it contains.
 */
data class BatchEnvelope<B: Batch>(
    val batch: B,
    val ranges: RangeSet<Long> = TreeRangeSet.create()
) {
    constructor(batch: B, range: Range<Long>):
        this(batch = batch, ranges = TreeRangeSet.create(listOf(range)))

    fun <C: Batch> withBatch(newBatch: C): BatchEnvelope<C> {
        return BatchEnvelope(newBatch, ranges)
    }
}
