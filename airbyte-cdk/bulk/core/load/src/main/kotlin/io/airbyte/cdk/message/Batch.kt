package io.airbyte.cdk.message

import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet
import java.nio.file.Path

/**
 * Represents an accumulated batch of records in some stage of processing.
 *
 * Emitted by the record accumulator per batch accumulated. Handled by
 * the batch processor, which may advanced the state and yield a new batch.
 */
interface Batch {
    enum class State {
        LOCAL,
        PERSISTED,
        COMPLETE
    }

    val state: State
}

class LocalStagedFile(
    val localPath: Path,
    val totalSizeBytes: Long,
    override val state: Batch.State = Batch.State.LOCAL
): Batch

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
