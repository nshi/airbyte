package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.state.StreamsManager
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

interface MessageQueueReader<K, T> {
    suspend fun readChunk(key: K, shard: Int = 0): Flow<T>
}

@Singleton
class DestinationMessageQueueReader(
    private val config: WriteConfiguration,
    private val messageQueue: DestinationMessageQueue,
    private val streamsManager: StreamsManager,
): MessageQueueReader<DestinationStream, DestinationRecordReadResult> {
    private val log = KotlinLogging.logger {}

    /**
     * Open a flow for consuming. Consumption continues until
     *   * MAX_BYTE_SIZE_PER_STREAM is reached (from message.size)
     *   * There is no available new data for the configured timeout
     *   * EOS is reached
     * Under these conditions, the DestinationMessage will be null,
     * and all subsequent calls will return null until the flow is
     * reopened. After EOS, all reads will return null.
     */
    override suspend fun readChunk(key: DestinationStream, shard: Int): Flow<DestinationRecordReadResult> = flow {
        val channel = messageQueue.getChannel(key, shard)

        // Keep emitting records until we
        //  * timeout
        //  * reach the max chunk size
        //  * reach the end of the stream
        var totalBytesRead = 0L
        var recordsRead = 0L
        val manager = streamsManager.getManager(key)
        val maxBytes = config.accumulatorMaxChunkSizeBytes
        while (totalBytesRead < maxBytes) {

            val wrapped = channel.receive(config.accumulatorReadTimeoutMs)
            if (wrapped == null) {
                log.info { "Read timed out for ${key.descriptor}:$shard" }
                emit(Timeout)
                return@flow
            }

            // Handle the next message
            when (wrapped) {
                is StreamRecordWrapped -> {
                    totalBytesRead += wrapped.sizeBytes
                    manager.countRecordOut(wrapped.sizeBytes)
                    emit(IndexedDestinationRecord(
                        record = wrapped.record,
                        index = wrapped.index,
                        sizeBytes = wrapped.sizeBytes
                    ))
                }
                is StreamCompleteWrapped -> {
                    channel.closed.set(true)
                    if ((0 until messageQueue.nShardsPerKey).all {
                            messageQueue.getChannel(key, it).closed.get()
                        }) {
                        manager.markConsumptionComplete()
                    }
                    emit(EndOfStream(wrapped.index))
                    return@flow
                }
            }
            recordsRead++
        }

        // Report that we've reached the end of the chunk
        log.info { "Reached end of chunk (${totalBytesRead}b/${maxBytes}b) for ${key.descriptor}:$shard" }
        emit(EndOfChunk)

        return@flow
    }
}
