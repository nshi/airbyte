package io.airbyte.cdk.task

import io.airbyte.cdk.write.Destination
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Provider
import jakarta.inject.Singleton

class SetupTask(
    private val destination: Destination,
    private val taskLauncher: DestinationTaskLauncher
): Task {
    override val concurrency = Task.Concurrency("setup")

    override suspend fun execute() {
        destination.setup()
        taskLauncher.enqueueOpenStreamTasks()
    }
}

@Singleton
@Secondary
class SetupTaskFactory(
    private val destination: Destination,
) {
    fun make(taskLauncher: DestinationTaskLauncher): SetupTask {
        return SetupTask(destination, taskLauncher)
    }
}
