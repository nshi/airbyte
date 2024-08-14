package io.airbyte.cdk.task

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.state.StreamManager
import io.airbyte.cdk.state.StreamsManager
import io.airbyte.cdk.write.StreamLoader
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Provider
import jakarta.inject.Singleton
import kotlinx.coroutines.delay
import org.apache.mina.util.ConcurrentHashSet


class CloseStreamTask(
    private val streamLoader: StreamLoader,
    private val streamManager: StreamManager,
    private val taskLauncher: DestinationTaskLauncher
): Task {
    companion object {
        val oncePerStream: ConcurrentHashSet<DestinationStream> = ConcurrentHashSet()
    }

    override val concurrency = Task.Concurrency("close-stream", 1)

    override suspend fun execute() {
        if (oncePerStream.contains(streamLoader.stream)) {
            return
        }
        oncePerStream.add(streamLoader.stream)
        streamLoader.close()
        streamManager.closeStream()
        taskLauncher.enqueueTeardownTask()
    }
}


@Singleton
@Secondary
class CloseStreamTaskFactory(
    private val streamsManager: StreamsManager,
) {
    fun make(
        taskLauncher: DestinationTaskLauncher,
        streamLoader: StreamLoader
    ): CloseStreamTask {
        return CloseStreamTask(
            streamLoader,
            streamsManager.getManager(streamLoader.stream),
            taskLauncher
        )
    }
}
