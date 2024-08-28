package io.airbyte.cdk.task

import com.google.common.collect.Range
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.message.BatchEnvelope
import io.airbyte.cdk.message.DestinationRecordWrapped
import io.airbyte.cdk.message.LocalStagedFile
import io.airbyte.cdk.message.MessageQueueReader
import io.airbyte.cdk.message.StreamCompleteWrapped
import io.airbyte.cdk.message.StreamRecordWrapped
import io.airbyte.cdk.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.nio.file.Files
import kotlin.io.path.bufferedWriter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.last
import kotlinx.coroutines.flow.runningFold
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield

class SpillToDiskTask(
    private val config: WriteConfiguration,
    private val queueReader: MessageQueueReader<DestinationStream, DestinationRecordWrapped>,
    private val streamLoader: StreamLoader,
    private val launcher: DestinationTaskLauncher
): Task {
    private val log = KotlinLogging.logger {}

    data class ReadResult(
        val range: Range<Long>? = null,
        val sizeBytes: Long = 0,
        val hasReadEndOfStream: Boolean = false
    )

    override suspend fun execute() {
        do {
            /** Create a temporary file to write the records to */
            val (path, bufferedReader) = withContext(Dispatchers.IO) {
                val path = Files.createTempFile(config.firstStageTmpFilePrefix, ".jsonl")
                Pair(path, path.bufferedWriter(charset = Charsets.UTF_8))
            }

            log.info { "Writing records to $path" }

            /** Read records from the queue and write them to the temporary file */
            val (range, sizeBytes, endOfStream) = bufferedReader.use {
                queueReader.readChunk(streamLoader.stream)
                    .runningFold(ReadResult()) { (range, sizeBytes, _), wrapped ->
                        when (wrapped) {
                            is StreamRecordWrapped -> {
                                val nextRange = if (range == null) {
                                    Range.singleton(wrapped.index)
                                } else {
                                    range.span(Range.singleton(wrapped.index))
                                }
                                it.write(wrapped.record.serialized)
                                ReadResult(nextRange, sizeBytes + wrapped.sizeBytes)
                            }

                            is StreamCompleteWrapped ->
                                return@runningFold ReadResult(range, sizeBytes, true)
                        }
                    }.flowOn(Dispatchers.IO)
            }.last()

            /** Handle the result */

            log.info { "Finished writing $range records (${sizeBytes}b) to $path" }

            // This could happen if the chunk only contained end-of-stream
            if (range == null) {
                return
            }

            val wrapped = BatchEnvelope(LocalStagedFile(path, sizeBytes), range)
            launcher.startProcessRecordsTask(streamLoader, wrapped)

            yield()
        } while (!endOfStream)
    }
}

@Singleton
class SpillToDiskTaskFactory(
    private val config: WriteConfiguration,
    private val queueReader: MessageQueueReader<DestinationStream, DestinationRecordWrapped>
) {
    fun make(
        taskLauncher: DestinationTaskLauncher,
        streamLoader: StreamLoader,
    ): SpillToDiskTask {
        return SpillToDiskTask(config, queueReader, streamLoader, taskLauncher)
    }
}

