package io.airbyte.cdk.task

import io.airbyte.cdk.write.Destination
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.write.StreamLoader
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Provider
import jakarta.inject.Singleton


class OpenStreamTask(
    private val streamLoader: StreamLoader,
    private val taskLauncher: DestinationTaskLauncher
): Task {
    override val concurrency = Task.Concurrency("open-stream", 4)

    override suspend fun execute() {
        streamLoader.open()
        taskLauncher.enqueueAccumulateRecordsTasks(streamLoader)
    }
}

@Singleton
@Secondary
class OpenStreamTaskFactory(
    private val destination: Destination,
)
{
    fun make(taskLauncher: DestinationTaskLauncher, stream: DestinationStream): OpenStreamTask {
        return OpenStreamTask(destination.getStreamLoader(stream), taskLauncher)
    }
}

