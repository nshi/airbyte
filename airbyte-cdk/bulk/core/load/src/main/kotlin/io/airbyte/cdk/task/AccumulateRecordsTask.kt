package io.airbyte.cdk.task

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.BatchEnvelope
import io.airbyte.cdk.message.DestinationRecordReadResult
import io.airbyte.cdk.message.EndOfChunk
import io.airbyte.cdk.message.EndOfStream
import io.airbyte.cdk.message.IndexedDestinationRecord
import io.airbyte.cdk.message.MessageQueue
import io.airbyte.cdk.message.MessageQueueReader
import io.airbyte.cdk.message.Timeout
import io.airbyte.cdk.state.StreamManager
import io.airbyte.cdk.state.StreamsManager
import io.airbyte.cdk.util.EmptyRange
import io.airbyte.cdk.util.ShardedRange
import io.airbyte.cdk.write.RecordAccumulator
import io.airbyte.cdk.write.StreamLoader
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import kotlinx.coroutines.flow.fold


class AccumulateRecordsTask(
    private val config: WriteConfiguration,
    private val streamLoader: StreamLoader,
    private val shard: Int,
    private val queueReader: MessageQueueReader<DestinationStream, DestinationRecordReadResult>,
    private val streamManager: StreamManager,
    private val taskLauncher: DestinationTaskLauncher,
    private val previousState: State = State()
): Task {
    /**
     * Kept in between complete batches (ie, if the
     * accumulator is forced to yield due to timeout).
     */
    data class State(
        val rangeRead: ShardedRange = EmptyRange,
        val accumulator: RecordAccumulator? = null
    )

    data class ReadContext(
        var rangeRead: ShardedRange,
        val accumulator: RecordAccumulator,
        var reenqueue: Boolean = true,
        var batchEnvelope: BatchEnvelope? = null
    )

    override val concurrency = Task.Concurrency(
        "accumulate-records",
        1,
        estimatedMemoryUsageBytes = config.getEstimatedAccumulatorUsageBytes()
    )

    override suspend fun execute() {
        val initialContext = ReadContext(
            previousState.rangeRead,
            previousState.accumulator ?: streamLoader.getRecordAccumulator(shard),
        )

        println("starting to read")
        val finalContext = queueReader.readChunk(streamLoader.stream, shard)
            .fold(initialContext) { ctx, queueMessage ->
                println("read message: $queueMessage")
                val batch = when (queueMessage) {
                    /* Batch will only be non-null if the accumulator completes */
                    is IndexedDestinationRecord -> {
                        ctx.rangeRead = ctx.rangeRead.withIndex(queueMessage.index)
                        println("next range: ${ctx.rangeRead} from index: ${queueMessage.index}")

                        ctx.accumulator.accept(queueMessage.record)
                    }

                    /* Flush must return a non-null batch. */
                    is EndOfStream -> {
                        /*
                            Each shard will eventually reach end of stream,
                            after which it should never re-enqueue.
                         */
                        ctx.reenqueue = false
                        ctx.rangeRead = ctx.rangeRead.withIndex(queueMessage.index)
                        ctx.accumulator.flush(endOfStream = true)
                    }

                    is EndOfChunk ->
                        ctx.accumulator.flush(endOfStream = false)

                    /* On timeout, assume the accumulator is still accumulating. */
                    is Timeout ->
                        Batch(state = Batch.State.ACCUMULATING)
                }

                if (batch != null) {
                    ctx.batchEnvelope = BatchEnvelope(shard, ctx.rangeRead, batch)
                    return@fold ctx
                } else {
                    ctx
                }
            }

        println("final context: $finalContext")
        streamManager.updateBatchState(finalContext.batchEnvelope!!)

        /*
            If the accumulator is not done with this batch, pass
            the state to the next task. Otherwise, enqueue process
            batch and reset the state for the next accumulator.
         */
        val nextState = if (finalContext.batchEnvelope!!.batch.state == Batch.State.ACCUMULATING) {
            State(finalContext.rangeRead, finalContext.accumulator)
        } else {
            taskLauncher.enqueueProcessBatchTask(streamLoader, finalContext.batchEnvelope!!)
            State()
        }

        if (finalContext.reenqueue) {
            println("reenqueueing")
            taskLauncher.reenqueueAccumulateRecordsTask(streamLoader, shard, nextState)
        } else {
            println("not reenqueueing")
        }
    }
}

@Singleton
@Secondary
class AccumulateRecordsTaskFactory(
    private val writeConfiguration: WriteConfiguration,
    private val queueReader: MessageQueueReader<DestinationStream, DestinationRecordReadResult>,
    private val streamsManager: StreamsManager,
) {
    fun make(taskLauncher: DestinationTaskLauncher,
             streamLoader: StreamLoader,
             shard: Int,
             nextState: AccumulateRecordsTask.State = AccumulateRecordsTask.State()
    ): AccumulateRecordsTask {
        return AccumulateRecordsTask(
            writeConfiguration,
            streamLoader,
            shard,
            queueReader,
            streamsManager.getManager(streamLoader.stream),
            taskLauncher,
            nextState
        )
    }
}
