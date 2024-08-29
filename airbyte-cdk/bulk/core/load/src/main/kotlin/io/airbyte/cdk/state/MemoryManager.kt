package io.airbyte.cdk.state

import io.airbyte.cdk.command.WriteConfiguration
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.delay

@Singleton
class MemoryManager {
    private val availableMemoryBytes: Long = Runtime.getRuntime().maxMemory()
    private var usedMemoryBytes = AtomicLong(0L)
    private val memoryLock = ReentrantLock()
    private val memoryLockCondition = memoryLock.newCondition()

    suspend fun reserveBlocking(memoryBytes: Long) {
        memoryLock.withLock {
            while (usedMemoryBytes.get() + memoryBytes > availableMemoryBytes) {
                memoryLockCondition.await()
            }
            usedMemoryBytes.addAndGet(memoryBytes)
        }
    }

    suspend fun reserveRatio(ratio: Double): Long {
        val estimatedSize = (availableMemoryBytes.toDouble() * ratio).toLong()
        reserveBlocking(estimatedSize)
        return estimatedSize
    }

    fun release(memoryBytes: Long) {
        memoryLock.withLock {
            usedMemoryBytes.addAndGet(-memoryBytes)
            memoryLockCondition.signalAll()
        }
    }
}
