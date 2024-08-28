package io.airbyte.cdk.task

import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.BatchEnvelope
import io.airbyte.cdk.message.Deserializer
import io.airbyte.cdk.message.DestinationMessage
import io.airbyte.cdk.message.DestinationRecord
import io.airbyte.cdk.message.DestinationRecordMessage
import io.airbyte.cdk.message.DestinationStreamComplete
import io.airbyte.cdk.message.LocalStagedFile
import io.airbyte.cdk.state.StreamManager
import io.airbyte.cdk.state.StreamsManager
import io.airbyte.cdk.write.StreamLoader
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import kotlin.io.path.bufferedReader


class ProcessRecordsTask(
    private val streamLoader: StreamLoader,
    private val streamManager: StreamManager,
    private val taskLauncher: DestinationTaskLauncher,
    private val fileEnvelope: BatchEnvelope<LocalStagedFile>,
    private val deserializer: Deserializer<DestinationMessage>,
): Task {
    override suspend fun execute() {
        val records = fileEnvelope.batch.localPath.bufferedReader(Charsets.UTF_8).lineSequence()
            .map {
                when (val record = deserializer.deserialize(it)) {
                    is DestinationRecordMessage -> record
                    else -> throw IllegalStateException("Expected record message, got ${record::class}")
                }
            }.takeWhile { it !is DestinationStreamComplete }
            .map { it as DestinationRecord }
            .iterator()
        val nextBatch = streamLoader.processRecords(records, fileEnvelope.batch.totalSizeBytes)
        val wrapped = fileEnvelope.withBatch(nextBatch)

        streamManager.updateBatchState(wrapped)

        if (nextBatch.state != Batch.State.COMPLETE) {
            taskLauncher.startProcessBatchTask(streamLoader, wrapped)
        }
    }
}

@Singleton
@Secondary
class ProcessRecordsTaskFactory(
    private val streamsManager: StreamsManager,
    private val deserializer: Deserializer<DestinationMessage>,
) {
    fun make(taskLauncher: DestinationTaskLauncher,
             streamLoader: StreamLoader,
             fileEnvelope: BatchEnvelope<LocalStagedFile>,
    ): ProcessRecordsTask {
        return ProcessRecordsTask(
            streamLoader,
            streamsManager.getManager(streamLoader.stream),
            taskLauncher,
            fileEnvelope,
            deserializer,
        )
    }
}
