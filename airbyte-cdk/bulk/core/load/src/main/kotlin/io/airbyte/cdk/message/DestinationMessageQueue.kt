package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.state.MemoryManager
import io.airbyte.cdk.state.StateManager
import io.airbyte.cdk.state.StreamsManager
import io.airbyte.cdk.util.ShardedIndex
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicReferenceArray
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

sealed class DestinationRecordWrapped
data class StreamRecordWrapped(
    val index: ShardedIndex,
    val sizeBytes: Long,
    val record: DestinationRecord
): DestinationRecordWrapped()
data class StreamCompleteWrapped(
    val index: ShardedIndex,
): DestinationRecordWrapped()

@Singleton
class DestinationMessageQueue(
    private val catalog: DestinationCatalog,
    private val config: WriteConfiguration,
    private val streamsManager: StreamsManager,
    private val memoryManager: MemoryManager,
): MessageQueue<DestinationStream, DestinationRecordWrapped> {
    override val nShardsPerKey: Int = config.getNumAccumulatorsPerStream(catalog.streams.size)

    private val channels: ConcurrentHashMap<DestinationStream.Descriptor,
        AtomicReferenceArray<QueueChannel<DestinationRecordWrapped>>> = ConcurrentHashMap()

    private val totalQueueSizeBytes = AtomicLong(0L)
    private val maxQueueSizeBytes: AtomicReference<Long?> = AtomicReference(null)

    override suspend fun acquireQueueBytesBlocking(bytes: Long) {
        if (maxQueueSizeBytes.get() == null) {
            val maxBytes = memoryManager.reserveRatio(config.maxMessageQueueMemoryUsageRatio)
            maxQueueSizeBytes.set(maxBytes)
        }
        val maxBytes = maxQueueSizeBytes.get()!!
        totalQueueSizeBytes.addAndGet(bytes)
        while (totalQueueSizeBytes.get() > maxBytes) {
            log.info { "Queue is full, waiting for space" }
            delay(config.memoryAvailabilityPollFrequencyMs)
        }
    }

    override suspend fun releaseQueueBytes(bytes: Long) {
        totalQueueSizeBytes.addAndGet(-bytes)
    }

    override suspend fun getChannel(
        key: DestinationStream,
        shard: Int
    ): QueueChannel<DestinationRecordWrapped> {
        return channels[key.descriptor]?.get(shard) ?:
            throw IllegalArgumentException("Reading from non-existent QueueChannel: ${key.descriptor}:$shard")
    }

    private val log = KotlinLogging.logger {}


    init {
        catalog.streams.forEach {
            channels[it.descriptor] = AtomicReferenceArray(nShardsPerKey)
            for (i in 0 until nShardsPerKey) {
                channels[it.descriptor]!![i] = QueueChannel()
            }
        }
    }
}
