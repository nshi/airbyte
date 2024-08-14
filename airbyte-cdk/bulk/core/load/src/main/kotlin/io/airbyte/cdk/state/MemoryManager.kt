package io.airbyte.cdk.state

import io.airbyte.cdk.command.WriteConfiguration
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.delay

@Singleton
class MemoryManager(
    private val config: WriteConfiguration
) {
    private val availableMemoryBytes: Long = Runtime.getRuntime().maxMemory()
    private var usedMemoryBytes = AtomicLong(0L)

    fun maybeReserve(memoryBytes: Long): Boolean {
        if (usedMemoryBytes.addAndGet(memoryBytes) > availableMemoryBytes) {
            usedMemoryBytes.addAndGet(-memoryBytes)
            return false
        }
        return true
    }

    suspend fun reserveBlocking(memoryBytes: Long) {
        usedMemoryBytes.addAndGet(memoryBytes)
        while (usedMemoryBytes.get() > availableMemoryBytes) {
            delay(config.memoryAvailabilityPollFrequencyMs)
        }
    }

    suspend fun reserveRatio(ratio: Double): Long {
        val estimatedSize = (availableMemoryBytes.toDouble() * ratio).toLong()
        reserveBlocking(estimatedSize)
        return estimatedSize
    }

    fun release(memoryBytes: Long) {
        val estimatedSize = (memoryBytes.toDouble() * config.accumulatorMemoryUsuageRatioPerRecord).toLong()
        usedMemoryBytes.addAndGet(-estimatedSize)
    }
}
