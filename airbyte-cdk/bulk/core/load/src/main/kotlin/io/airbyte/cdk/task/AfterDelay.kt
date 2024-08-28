package io.airbyte.cdk.task

import kotlinx.coroutines.delay

class AfterDelay(
    private val taskRunner: TaskRunner,
    private val task: Task,
    private val delayMs: Long
): Task {
    override val concurrency: Task.Concurrency = Task.Concurrency("with-delay")

    override suspend fun execute() {
        delay(delayMs)
        taskRunner.enqueue(task)
    }
}
