package io.airbyte.cdk.command

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import kotlin.math.min

interface WriteConfiguration {
    /**
     * Concurrency and read settings for
     * the accumulators.
     */
    val maxNumAccumulators: Int
    val accumulatorReadTimeoutMs: Long
    val accumulatorMaxChunkSizeBytes: Long
    val accumulatorPollFrequencyMs: Long
    val accumulatorMemoryUsuageRatioPerRecord: Double

    fun getNumAccumulatorsPerStream(nStreams: Int): Int {
        val log = KotlinLogging.logger {}

        log.info { "Calculating number of accumulators per stream for nStreams=$nStreams and maxNumAccumualtors=$maxNumAccumulators" }
        if (nStreams == 0) {
            throw IllegalArgumentException("Catalog must have at least one stream")
        }

        return min(maxNumAccumulators / nStreams, 1)
    }

    fun getEstimatedAccumulatorUsageBytes(): Long {
        return (accumulatorMaxChunkSizeBytes * accumulatorMemoryUsuageRatioPerRecord).toLong()
    }

    /**
     * Work queue settings.
     */
    val noTaskAvailableWaitTimeMs: Long
    val reenqueueTaskWaitTimeMs: Long

    /**
     * Memory queue settings
     */
    val maxMessageQueueMemoryUsageRatio: Double
    val memoryAvailabilityPollFrequencyMs: Long
}

@Singleton
@Secondary
open class DefaultWriteConfiguration: WriteConfiguration {
    override val maxNumAccumulators: Int = 2
    override val accumulatorReadTimeoutMs: Long = 1000L
    override val accumulatorMaxChunkSizeBytes: Long = 200L * 1024L * 1024L
    override val accumulatorPollFrequencyMs: Long = 1000L
    override val accumulatorMemoryUsuageRatioPerRecord: Double = 1.0

    override val noTaskAvailableWaitTimeMs: Long = 1000L
    override val reenqueueTaskWaitTimeMs: Long = 5000L

    override val maxMessageQueueMemoryUsageRatio: Double = 0.2
    override val memoryAvailabilityPollFrequencyMs: Long = 1000L
}
