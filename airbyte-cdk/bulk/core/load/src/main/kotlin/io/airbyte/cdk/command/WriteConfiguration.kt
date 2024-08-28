package io.airbyte.cdk.command

import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

interface WriteConfiguration {
    /**
     * Batch accumulation settings.
     */
    val chunkSizeBytes: Long

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

    /**
     * Disk accumulation config
     */
    val firstStageTmpFilePrefix: String
}

@Singleton
@Secondary
open class DefaultWriteConfiguration: WriteConfiguration {
    override val chunkSizeBytes: Long = 200L * 1024L * 1024L

    override val noTaskAvailableWaitTimeMs: Long = 1000L
    override val reenqueueTaskWaitTimeMs: Long = 5000L

    override val maxMessageQueueMemoryUsageRatio: Double = 0.2
    override val memoryAvailabilityPollFrequencyMs: Long = 1000L

    override val firstStageTmpFilePrefix = "aibyte-cdk-load/spilled-records"
}
