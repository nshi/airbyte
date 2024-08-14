package io.airbyte.cdk.state

import com.google.common.collect.Range
import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.message.BatchEnvelope
import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.util.ShardedRangeSet
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReferenceArray

interface StreamsManager {
    fun getManager(stream: DestinationStream): StreamManager
    fun openStreamCount(): Int
}


class DefaultStreamsManager(
    private val streamManagers: ConcurrentHashMap<DestinationStream, StreamManager>
): StreamsManager {
    override fun getManager(stream: DestinationStream): StreamManager {
        return streamManagers[stream] ?:
            throw IllegalArgumentException("Stream not found: $stream")
    }

    override fun openStreamCount(): Int {
        return streamManagers.values.count { !it.isStreamClosed() }
    }
}

interface StreamManager {
    fun countRecordIn(sizeBytes: Long): Long
    fun countRecordOut(sizeBytes: Long)
    fun markPublishComplete()
    fun markConsumptionComplete()
    fun markCheckpoint(): Pair<Long, Long>
    fun updateBatchState(batch: BatchEnvelope)
    fun isBatchProcessingComplete(): Boolean
    fun areRecordsPersistedUntil(index: Long): Boolean
    fun closeStream()
    fun isStreamClosed(): Boolean
}


class DefaultStreamManager(
    private val numShards: Int,
    val stream: DestinationStream
): StreamManager {
    private val log = KotlinLogging.logger {}

    private val streamStatus: StreamStatus = StreamStatus()
    private val rangesState: ConcurrentHashMap<Batch.State, AtomicReferenceArray<ShardedRangeSet>> = ConcurrentHashMap()

    init {
        Batch.State.entries.forEach {
            rangesState[it] = AtomicReferenceArray(numShards)
        }
    }

    override fun countRecordIn(sizeBytes: Long): Long {
        val index = streamStatus.recordsPublished.getAndIncrement()
        streamStatus.totalBytes.addAndGet(sizeBytes)
        streamStatus.enqueuedSize.addAndGet(sizeBytes)
        return index
    }

    override fun countRecordOut(sizeBytes: Long) {
        streamStatus.recordsConsumed.incrementAndGet()
        streamStatus.enqueuedSize.addAndGet(-sizeBytes)
    }

    override fun markPublishComplete() {
        streamStatus.publishComplete.set(true)
    }

    override fun markConsumptionComplete() {
        streamStatus.consumptionComplete.set(true)
    }

    /**
     * Mark a checkpoint in the stream and return the current
     * index and the number of records since the last one.
     */
    override fun markCheckpoint(): Pair<Long, Long> {
        val index = streamStatus.recordsPublished.get()
        val lastCheckpoint = streamStatus.lastCheckpoint.getAndSet(index)
        return Pair(index, index - lastCheckpoint)
    }

    override fun updateBatchState(batch: BatchEnvelope) {
        val stateRanges = rangesState[batch.batch.state] ?:
            throw IllegalArgumentException("Invalid batch state: ${batch.batch.state}")

        batch.ranges.forEach { (shard, batchRanges) ->
            val updatedRanges = stateRanges.updateAndGet(shard) { currentRanges ->
                currentRanges?.apply { addAll(batchRanges) } ?: batchRanges
            }

            log.info { "Updated ranges for $stream($shard)[${batch.batch.state}]: $updatedRanges" }
        }
    }

    private fun calculateEndOfRange(index: Long, shard: Int): Long {
        val shards = numShards.toLong()
        val endOfRange = (index / shards) * shards + (shard.toLong())
        if (endOfRange > index) {
            return endOfRange - shards
        }
        return endOfRange
    }

    private fun isProcessingCompleteForState(index: Long, state: Batch.State): Boolean {

        val completeRanges = rangesState[state]!!
        return (0 until numShards).all {
            val expectedUnshardedRange = Range.closed(it.toLong(), calculateEndOfRange(index, it) - 1)
            completeRanges.get(it).unshardedRangeSet.encloses(expectedUnshardedRange)
        }
    }

    override fun isBatchProcessingComplete(): Boolean {
        return isProcessingCompleteForState(streamStatus.recordsPublished.get(), Batch.State.COMPLETE)
    }

    override fun areRecordsPersistedUntil(index: Long): Boolean {
        return isProcessingCompleteForState(index, Batch.State.PERSISTED)
    }

    override fun closeStream() {
        streamStatus.closed.set(true)
    }

    override fun isStreamClosed(): Boolean {
        return streamStatus.closed.get()
    }

}


@Factory
class StreamsManagerFactory(
    private val catalog: DestinationCatalog,
    private val writeConfig: WriteConfiguration
) {
    @Singleton
    fun make(): StreamsManager {
        val hashMap = ConcurrentHashMap<DestinationStream, StreamManager>()
        val numShards = writeConfig.getNumAccumulatorsPerStream(catalog.streams.size)
        catalog.streams.forEach {
            hashMap[it] = DefaultStreamManager(numShards, it)
        }
        return DefaultStreamsManager(hashMap)
    }
}
