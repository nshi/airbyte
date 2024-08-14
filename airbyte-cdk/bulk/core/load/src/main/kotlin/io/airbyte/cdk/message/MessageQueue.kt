package io.airbyte.cdk.message


import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

interface MessageQueue<K, T> {
    val nShardsPerKey: Int

    suspend fun acquireQueueBytesBlocking(bytes: Long)
    suspend fun releaseQueueBytes(bytes: Long)
    suspend fun getChannel(key: K, shard: Int): QueueChannel<T>
}

data class QueueChannel<T>(
    val channel: Channel<T> = Channel(Channel.UNLIMITED),
    val closed: AtomicBoolean = AtomicBoolean(false)
) {
    suspend fun send(message: T) {
        if (closed.get()) {
            throw IllegalStateException("Send to closed QueueChannel")
        }
        channel.send(message)
    }

    suspend fun receive(timeoutMs: Long): T? {
        if (closed.get()) {
            throw IllegalStateException("Receive from closed QueueChannel")
        }
        return withTimeoutOrNull(timeoutMs) {
            channel.receive()
        }
    }
}
