package io.airbyte.cdk.task

import jakarta.inject.Singleton

interface Task {
    suspend fun execute()
}

interface TaskLauncher {
    val taskRunner: TaskRunner

    suspend fun start()
    suspend fun stop() {
        taskRunner.enqueue(Done())
    }
}

@Singleton
class Done: Task {
    override suspend fun execute() {
        throw IllegalStateException("The Done() task cannot be executed")
    }
}

