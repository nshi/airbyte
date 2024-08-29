package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.state.StateManager
import io.airbyte.cdk.state.StreamsManager
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton


interface MessageQueueWriter<T> {
    suspend fun publish(message: T, sizeBytes: Long)
}

@Singleton
class DestinationMessageQueueWriter(
    private val catalog: DestinationCatalog,
    private val messageQueue: MessageQueue<DestinationStream, DestinationRecordWrapped>,
    private val streamsManager: StreamsManager,
    private val stateManager: StateManager
): MessageQueueWriter<DestinationMessage> {
    private val log = KotlinLogging.logger {}

    /**
     * Deserialize and route the message to the appropriate channel.
     *
     * NOTE: Not thread-safe! Only a single writer should publish to the queue.
     */
    override suspend fun publish(message: DestinationMessage, sizeBytes: Long) {
        when (message) {
            /* If the input message represents a record. */
            is DestinationRecordMessage -> {
                val manager = streamsManager.getManager(message.stream)
                val index = manager.countRecordIn(sizeBytes)
                when (message) {
                    /* If a data record */
                    is DestinationRecord -> {
                        val wrapped = StreamRecordWrapped(
                            index = index,
                            sizeBytes = sizeBytes,
                            record = message
                        )
                        messageQueue.getChannel(message.stream).send(wrapped)
                    }

                    /* If an end-of-stream marker. */
                    is DestinationStreamComplete -> {
                        val wrapped = StreamCompleteWrapped(index)
                        messageQueue.getChannel(message.stream).send(wrapped)
                    }
                }
            }

            is DestinationStateMessage -> {
                when (message) {
                    is DestinationStreamState -> {
                        val stream = message.streamState.stream
                        val manager = streamsManager.getManager(stream)
                        val (currentIndex, countSinceLast) = manager.markCheckpoint()
                        val messageWithCount = message.withDestinationStats(
                            DestinationStateMessage.Stats(countSinceLast)
                        )
                        stateManager.addStreamState(stream, currentIndex, messageWithCount)

                    }
                    is DestinationGlobalState -> {
                        val streamWithIndexAndCount = catalog.streams.map { stream ->
                            val manager = streamsManager.getManager(stream)
                            val (currentIndex, countSinceLast) = manager.markCheckpoint()
                            Triple(stream, currentIndex, countSinceLast)
                        }
                        val totalCount = streamWithIndexAndCount.sumOf { it.third }
                        val messageWithCount = message.withDestinationStats(
                            DestinationStateMessage.Stats(totalCount)
                        )
                        val streamIndexes = streamWithIndexAndCount.map { it.first to it.second }
                        stateManager.addGlobalState(streamIndexes, messageWithCount)
                    }
                }
            }

            is Undefined -> {} // Do nothing
        }
    }
}
