package io.airbyte.cdk.task

import io.airbyte.cdk.write.Destination
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

class SetupTask(
    private val destination: Destination,
    private val taskLauncher: DestinationTaskLauncher
): Task {
    override suspend fun execute() {
        destination.setup()
        taskLauncher.startOpenStreamTasks()
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
