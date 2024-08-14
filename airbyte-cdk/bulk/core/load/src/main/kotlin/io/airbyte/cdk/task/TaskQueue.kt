package io.airbyte.cdk.task

import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.state.MemoryManager
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield

@Singleton
class TaskQueue(
    private val config: WriteConfiguration,
    private val memoryManager: MemoryManager
) {

    private val queue = Channel<Task>(Channel.UNLIMITED)

    suspend fun enqueue(task: Task) {
        queue.send(task)
    }

    suspend fun run() = coroutineScope {
        val log = KotlinLogging.logger {}

        val runningTaskCounters = mutableMapOf<String, AtomicInteger>()
        val totalTaskRunCounters = mutableMapOf<String, AtomicInteger>()

        while (true) {
            yield()

            /**
             * Acquire a task. If no tasks are available,
             * wait for the configured timeout.
             */
            val task = withTimeoutOrNull(config.noTaskAvailableWaitTimeMs) {
                queue.receive()
            }

            if (task == null) {
                continue
            }

            if (task is Done) {
                log.info { "Task queue received Done() task, exiting"}
                return@coroutineScope
            }

            /**
             * Attempt to reserve memory for the task as needed.
             */
            if (task.concurrency.estimatedMemoryUsageBytes > 0) {
                if (!memoryManager.maybeReserve(task.concurrency.estimatedMemoryUsageBytes)) {
                    log.info { "Re-enqueuing task: $task: insufficient memory" }
                    queue.send(AfterDelay(this@TaskQueue, task, config.reenqueueTaskWaitTimeMs))
                    continue
                }
            }

            /**
             * If `concurrency.limit` instances of this task are
             * running, re-enqueue the task.
             */
            val runningCounter = if (task.concurrency.limit > 0) {
                val counter = runningTaskCounters.getOrPut(task.concurrency.id) {
                    AtomicInteger(0)
                }

                if (counter.get() >= task.concurrency.limit) {
                    // Re-enqueue the task to give it another chance.
                    log.info { "Re-enqueuing task: $task: ${counter.get()} running >= ${task.concurrency.limit} limit" }
                    queue.send(AfterDelay(this@TaskQueue, task, config.reenqueueTaskWaitTimeMs))

                    continue
                }

                counter
            } else {
                null
            }

            /**
             * If the task has run `concurrency.maxRuns` times,
             * drop the task.
             */
            val totalCounter = if (task.concurrency.maxRuns > 0) {
                val counter = totalTaskRunCounters.getOrPut(task.concurrency.id) {
                    AtomicInteger(0)
                }

                val nRuns = counter.get()
                val limit = task.concurrency.maxRuns
                if (nRuns >= limit) {
                    log.info { "Dropping task: $task: $nRuns runs > $limit max" }
                    continue
                }

                counter
            } else {
                null
            }

            /**
             * Launch the task concurrently and update counters.
             */
            launch {
                runningCounter?.incrementAndGet()
                totalCounter?.incrementAndGet()
                log.info { "Executing task: $task" }
                task.execute()
                runningCounter?.decrementAndGet()
            }
        }
    }
}
