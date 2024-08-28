package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.state.MemoryManager
import io.airbyte.cdk.state.StreamsManager
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicReferenceArray
import kotlinx.coroutines.delay

sealed class DestinationRecordWrapped: Sized
data class StreamRecordWrapped(
    val index: Long,
    override val sizeBytes: Long,
    val record: DestinationRecord
): DestinationRecordWrapped()
data class StreamCompleteWrapped(
    val index: Long,
): DestinationRecordWrapped() {
    override val sizeBytes: Long = 0L
}

@Singleton
class DestinationMessageQueue(
    catalog: DestinationCatalog,
    private val config: WriteConfiguration,
    private val memoryManager: MemoryManager,
    private val queueChannelFactory: QueueChannelFactory<DestinationRecordWrapped>
): MessageQueue<DestinationStream, DestinationRecordWrapped> {
    private val channels: ConcurrentHashMap<DestinationStream.Descriptor,
        QueueChannel<DestinationRecordWrapped>> = ConcurrentHashMap()

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
    ): QueueChannel<DestinationRecordWrapped> {
        return channels[key.descriptor] ?:
            throw IllegalArgumentException("Reading from non-existent QueueChannel: ${key.descriptor}")
    }

    private val log = KotlinLogging.logger {}


    init {
        catalog.streams.forEach {
            channels[it.descriptor] = queueChannelFactory.make(this)
        }
    }
}
