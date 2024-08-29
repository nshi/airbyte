package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.WriteConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

interface MessageQueueReader<K, T> {
    suspend fun readChunk(key: K): Flow<T>
}

@Singleton
class DestinationMessageQueueReader(
    private val config: WriteConfiguration,
    private val messageQueue: DestinationMessageQueue,
): MessageQueueReader<DestinationStream, DestinationRecordWrapped> {
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
    override suspend fun readChunk(key: DestinationStream): Flow<DestinationRecordWrapped> = flow {
        // Keep emitting records until we
        //  * timeout
        //  * reach the max chunk size
        //  * reach the end of the stream
        var totalBytesRead = 0L
        var recordsRead = 0L
        val maxBytes = config.recordBatchSizeBytes
        while (totalBytesRead < maxBytes) {

            when (val wrapped = messageQueue.getChannel(key).receive()) {
                is StreamRecordWrapped -> {
                    totalBytesRead += wrapped.sizeBytes
                    emit(wrapped)
                }
                is StreamCompleteWrapped -> {
                    messageQueue.getChannel(key).closed.set(true)
                    emit(wrapped)
                    return@flow
                }
            }
            recordsRead++
        }

        return@flow
    }
}
