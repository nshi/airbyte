package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.state.StateManager
import io.airbyte.cdk.state.StreamsManager
import io.airbyte.cdk.util.ShardedIndex
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton


interface MessageQueueWriter<T> {
    suspend fun publish(message: T, sizeBytes: Long)
}

@Singleton
class DestinationMessageQueueWriter(
    private val catalog: DestinationCatalog,
    private val messageQueue: DestinationMessageQueue,
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
                val count = manager.countRecordIn(sizeBytes)
                val nShards = messageQueue.nShardsPerKey
                val index = ShardedIndex(count, nShards)

                when (message) {
                    /* If a data record */
                    is DestinationRecord -> {
                        messageQueue.acquireQueueBytesBlocking(sizeBytes)
                        val shard = (count % nShards.toLong()).toInt()
                        val wrapped = StreamRecordWrapped(
                            index = index,
                            sizeBytes = sizeBytes,
                            record = message
                        )
                        messageQueue.getChannel(message.stream, shard).send(wrapped)
                    }

                    /* If an end-of-stream marker. */
                    is DestinationStreamComplete -> {
                        log.info { "Publishing end of stream for ${message.stream}" }
                        manager.markPublishComplete()
                        val wrapped = StreamCompleteWrapped(index)
                        for (shard in 0 until nShards) {
                            messageQueue.getChannel(message.stream, shard).send(wrapped)
                        }
                    }
                }
            }

            is DestinationStateMessage -> {
                val (streams, isGlobal) = when (message) {
                    is DestinationStreamState -> Pair(listOf(message.streamState.stream), false)
                    is DestinationGlobalState -> Pair(catalog.streams, true)
                }
                streams.forEach { stream ->
                    val manager = streamsManager.getManager(stream)
                    val (currentIndex, countSinceLast) = manager.markCheckpoint()
                    val messageWithCount =
                        message.withDestinationStats(DestinationStateMessage.Stats(countSinceLast))
                    log.info { "Received state message for $stream at index $currentIndex" }
                    stateManager.addState(stream, messageWithCount, currentIndex, isGlobal)
                }
            }

            is Undefined -> {} // Do nothing
        }
    }
}
