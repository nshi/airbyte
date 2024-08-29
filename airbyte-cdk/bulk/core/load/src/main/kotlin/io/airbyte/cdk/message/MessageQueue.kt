package io.airbyte.cdk.message


import io.airbyte.cdk.command.WriteConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.channels.Channel

interface Sized {
    val sizeBytes: Long
}

interface MessageQueue<K, T: Sized> {
    suspend fun acquireQueueBytesBlocking(bytes: Long)
    suspend fun releaseQueueBytes(bytes: Long)
    suspend fun getChannel(key: K): QueueChannel<T>
}

interface QueueChannel<T: Sized> {
    val config: WriteConfiguration
    val messageQueue: MessageQueue<*, T>
    val channel: Channel<T>
    val closed: AtomicBoolean

    suspend fun send(message: T) {
        if (closed.get()) {
            throw IllegalStateException("Send to closed QueueChannel")
        }
        val estimatedSize = message.sizeBytes * config.estimatedRecordMemoryOverheadRatio
        messageQueue.acquireQueueBytesBlocking(estimatedSize.toLong())
        channel.send(message)
    }

    suspend fun receive(): T {
        if (closed.get()) {
            throw IllegalStateException("Receive from closed QueueChannel")
        }
        val message = channel.receive()
        val estimatedSize = message.sizeBytes * config.estimatedRecordMemoryOverheadRatio
        messageQueue.releaseQueueBytes(estimatedSize.toLong())
        return message
    }
}

interface QueueChannelFactory<T: Sized> {
    fun make(messageQueue: MessageQueue<*, T>): QueueChannel<T>
}


class DefaultQueueChannel<T: Sized> (
    override val config: WriteConfiguration,
    override val messageQueue: MessageQueue<*, T>
) : QueueChannel<T> {
    override val channel = Channel<T>(Channel.UNLIMITED)
    override val closed = AtomicBoolean(false)
}

@Singleton
class DefaultQueueChannelFactory(
    private val config: WriteConfiguration
): QueueChannelFactory<DestinationRecordWrapped> {
    override fun make(messageQueue: MessageQueue<*, DestinationRecordWrapped>): QueueChannel<DestinationRecordWrapped> =
        DefaultQueueChannel(config, messageQueue)
}




