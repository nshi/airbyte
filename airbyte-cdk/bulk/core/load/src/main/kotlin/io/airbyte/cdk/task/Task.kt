package io.airbyte.cdk.task

import jakarta.inject.Singleton
import kotlin.reflect.KClass

interface Task {
    abstract class Companion {
        abstract val metadata: Metadata
    }

    data class Concurrency(
        val id: String,
        val limit: Int=0,
        val maxRuns: Int=0,
        val exclusions: Set<String> = emptySet(),
        val estimatedMemoryUsageBytes: Long = 0L
    )

    data class Metadata(
        val name: String,
        val description: String? = null,
        val steps: List<Pair<String, KClass<out Task>>> = emptyList()
    )

    val concurrency: Concurrency

    suspend fun execute()
}

interface TaskLauncher {
    val taskQueue: TaskQueue

    suspend fun start()
    suspend fun stop() {
        taskQueue.enqueue(Done())
    }
}

@Singleton
class Done: Task {
    override val concurrency = Task.Concurrency("n/a")

    override suspend fun execute() {
        throw IllegalStateException("The Done() task cannot be executed")
    }
}

