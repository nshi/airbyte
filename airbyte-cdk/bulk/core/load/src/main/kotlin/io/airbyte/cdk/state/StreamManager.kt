package io.airbyte.cdk.state

import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet
import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.BatchEnvelope
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext

interface StreamsManager {
    fun getManager(stream: DestinationStream): StreamManager
    suspend fun awaitAllStreamsComplete()
}

class DefaultStreamsManager(
    private val streamManagers: ConcurrentHashMap<DestinationStream, StreamManager>
): StreamsManager {
    override fun getManager(stream: DestinationStream): StreamManager {
        return streamManagers[stream] ?:
            throw IllegalArgumentException("Stream not found: $stream")
    }

    override suspend fun awaitAllStreamsComplete() {
        streamManagers.forEach { (_, manager) ->
            manager.awaitStreamClosed()
        }
    }
}

interface StreamManager {
    fun countRecordIn(sizeBytes: Long): Long
    fun markCheckpoint(): Pair<Long, Long>
    fun <B: Batch> updateBatchState(batch: BatchEnvelope<B>)
    fun isBatchProcessingComplete(): Boolean
    fun areRecordsPersistedUntil(index: Long): Boolean

    fun markClosed()
    fun streamIsClosed(): Boolean
    suspend fun awaitStreamClosed()
}


class DefaultStreamManager(
 val stream: DestinationStream,
): StreamManager {
    private val log = KotlinLogging.logger {}

    data class StreamStatus(
        val recordCount: AtomicLong = AtomicLong(0),
        val totalBytes: AtomicLong = AtomicLong(0),
        val enqueuedSize: AtomicLong = AtomicLong(0),
        val lastCheckpoint: AtomicLong = AtomicLong(0L),
        val closedLatch: CountDownLatch = CountDownLatch(1),
    )


    private val streamStatus: StreamStatus = StreamStatus()
    private val rangesState: ConcurrentHashMap<Batch.State, RangeSet<Long>> = ConcurrentHashMap()

    init {
        Batch.State.entries.forEach {
            rangesState[it] = TreeRangeSet.create()
        }
    }

    override fun countRecordIn(sizeBytes: Long): Long {
        val index = streamStatus.recordCount.getAndIncrement()
        streamStatus.totalBytes.addAndGet(sizeBytes)
        streamStatus.enqueuedSize.addAndGet(sizeBytes)
        return index
    }

    /**
     * Mark a checkpoint in the stream and return the current
     * index and the number of records since the last one.
     */
    override fun markCheckpoint(): Pair<Long, Long> {
        val index = streamStatus.recordCount.get()
        val lastCheckpoint = streamStatus.lastCheckpoint.getAndSet(index)
        return Pair(index, index - lastCheckpoint)
    }

    override fun <B: Batch> updateBatchState(batch: BatchEnvelope<B>) {
        val stateRanges = rangesState[batch.batch.state] ?:
            throw IllegalArgumentException("Invalid batch state: ${batch.batch.state}")

        stateRanges.addAll(batch.ranges)
        log.info { "Updated ranges for $stream[${batch.batch.state}]: $stateRanges" }
    }

    private fun isProcessingCompleteForState(index: Long, state: Batch.State): Boolean {

        val completeRanges = rangesState[state]!!
        return completeRanges.encloses(Range.closed(0L, index - 1))
    }

    override fun isBatchProcessingComplete(): Boolean {
        return isProcessingCompleteForState(streamStatus.recordCount.get(), Batch.State.COMPLETE)
    }

    override fun areRecordsPersistedUntil(index: Long): Boolean {
        return isProcessingCompleteForState(index, Batch.State.PERSISTED)
    }

    override fun markClosed() {
        streamStatus.closedLatch.countDown()
    }

    override fun streamIsClosed(): Boolean {
        return streamStatus.closedLatch.count == 0L
    }

    override suspend fun awaitStreamClosed() {
        withContext(Dispatchers.IO) {
            streamStatus.closedLatch.await()
        }
    }
}


@Factory
class StreamsManagerFactory(
    private val catalog: DestinationCatalog,
) {
    @Singleton
    fun make(): StreamsManager {
        val hashMap = ConcurrentHashMap<DestinationStream, StreamManager>()
        catalog.streams.forEach {
            hashMap[it] = DefaultStreamManager(it)
        }
        return DefaultStreamsManager(hashMap)
    }
}
