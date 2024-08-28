package io.airbyte.cdk.message


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
    val messageQueue: MessageQueue<*, T>
    val channel: Channel<T>
    val closed: AtomicBoolean

    suspend fun send(message: T) {
        if (closed.get()) {
            throw IllegalStateException("Send to closed QueueChannel")
        }
        messageQueue.acquireQueueBytesBlocking(message.sizeBytes)
        channel.send(message)
    }

    suspend fun receive(): T {
        if (closed.get()) {
            throw IllegalStateException("Receive from closed QueueChannel")
        }
        messageQueue.releaseQueueBytes(channel.receive().sizeBytes)
        return channel.receive()
    }
}

interface QueueChannelFactory<T: Sized> {
    fun make(messageQueue: MessageQueue<*, T>): QueueChannel<T>
}


class DefaultQueueChannel<T: Sized> (
    override val messageQueue: MessageQueue<*, T>
) : QueueChannel<T> {
    override val channel = Channel<T>()
    override val closed = AtomicBoolean(false)
}

@Singleton
class DefaultQueueChannelFactory: QueueChannelFactory<DestinationRecordWrapped> {
    override fun make(messageQueue: MessageQueue<*, DestinationRecordWrapped>): QueueChannel<DestinationRecordWrapped> =
        DefaultQueueChannel(messageQueue)
}




