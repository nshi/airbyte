package io.airbyte.cdk.task

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield

@Singleton
class TaskRunner {
    private val queue = Channel<Task>(Channel.UNLIMITED)

    suspend fun enqueue(task: Task) {
        queue.send(task)
    }

    suspend fun run() = coroutineScope {
        val log = KotlinLogging.logger {}

        while (true) {
            val task = queue.receive()

            if (task is Done) {
                log.info { "Task queue received Done() task, exiting"}
                return@coroutineScope
            }

            /**
             * Launch the task concurrently and update counters.
             */
            launch {
                log.info { "Executing task: $task" }
                task.execute()
            }

            yield()
        }
    }
}
