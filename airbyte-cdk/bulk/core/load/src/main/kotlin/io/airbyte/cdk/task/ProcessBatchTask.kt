package io.airbyte.cdk.task

import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.BatchEnvelope
import io.airbyte.cdk.state.StreamManager
import io.airbyte.cdk.state.StreamsManager
import io.airbyte.cdk.write.StreamLoader
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton


class ProcessBatchTask(
    private val batchEnvelope: BatchEnvelope<*>,
    private val streamLoader: StreamLoader,
    private val streamManager: StreamManager,
    private val taskLauncher: DestinationTaskLauncher
): Task {
    override suspend fun execute() {
        val nextBatch = streamLoader.processBatch(batchEnvelope.batch)
        val nextWrapped = batchEnvelope.withBatch(nextBatch)
        streamManager.updateBatchState(nextWrapped)

        if (nextBatch.state != Batch.State.COMPLETE) {
            taskLauncher.startProcessBatchTask(streamLoader, nextWrapped)
        } else if (streamManager.isBatchProcessingComplete()) {
            taskLauncher.startCloseStreamTasks(streamLoader)
        }
    }
}


@Singleton
@Secondary
class ProcessBatchTaskFactory(
    private val streamsManager: StreamsManager,
) {
    fun make(
        taskLauncher: DestinationTaskLauncher,
        streamLoader: StreamLoader,
        batchEnvelope: BatchEnvelope<*>
    ): ProcessBatchTask {
        return ProcessBatchTask(
            batchEnvelope,
            streamLoader,
            streamsManager.getManager(streamLoader.stream),
            taskLauncher
        )
    }
}
