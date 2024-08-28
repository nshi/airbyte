package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.state.StateManager
import io.airbyte.cdk.state.StreamManager
import io.airbyte.cdk.state.StreamsManager
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Prototype
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Stream
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource

@MicronautTest
class DestinationMessageQueueWriterTest {
    @Inject lateinit var testContextFactory: TestContextFactory

    @Prototype
    @Primary
    class MockStateManager: StateManager {
        override fun addStreamState(
            stream: DestinationStream,
            index: Long,
            stateMessage: DestinationStateMessage
        ) {
            TODO("Not yet implemented")
        }

        override fun addGlobalState(
            streamIndexes: List<Pair<DestinationStream, Long>>,
            stateMessage: DestinationStateMessage
        ) {
            TODO("Not yet implemented")
        }

        override fun flushStates() {
            TODO("Not yet implemented")
        }

    }

    @Prototype
    @Primary
    class MockStreamsManager: StreamsManager {
        override fun getManager(stream: DestinationStream): StreamManager {
            TODO()
        }

        override fun openStreamCount(): Int {
            TODO()
        }
    }

    class MockQueueChannel(
        override val messageQueue: MessageQueue<*, DestinationRecordWrapped>,
        override val channel: Channel<DestinationRecordWrapped>,
        override val closed: AtomicBoolean
    ) : QueueChannel<DestinationRecordWrapped> {
        override suspend fun send(message: DestinationRecordWrapped) {
            // no-op
        }

        override suspend fun receive(timeoutMs: Long): DestinationRecordWrapped? {
            return null
        }
    }

    @Singleton
    @Primary
    class MockQueueChannelFactory: QueueChannelFactory<DestinationRecordWrapped> {
        override fun make(messageQueue: MessageQueue<*, DestinationRecordWrapped>): QueueChannel<DestinationRecordWrapped> {
            return MockQueueChannel(messageQueue, Channel(), AtomicBoolean(false))
        }
    }

    class MockMessageQueue(
        override val nShardsPerKey: Int
    ) : MessageQueue<DestinationStream, DestinationRecordWrapped> {
        override suspend fun acquireQueueBytesBlocking(bytes: Long) {
            TODO("Not yet implemented")
        }

        override suspend fun releaseQueueBytes(bytes: Long) {
            TODO("Not yet implemented")
        }

        override suspend fun getChannel(
            key: DestinationStream,
            shard: Int
        ): QueueChannel<DestinationRecordWrapped> {
            TODO("Not yet implemented")
        }
    }

    @Prototype
    @Primary
    class MockMessageQueueFactory() {
        fun make(nShardsPerKey: Int) = MockMessageQueue(nShardsPerKey)
    }

    data class TestContext(
        val writer: DestinationMessageQueueWriter,
        val streamsManager: StreamsManager,
        val stateManager: StateManager
    )

    @Prototype
    class TestContextFactory(
        private val catalog: DestinationCatalog,
        private val messageQueue: MockMessageQueueFactory,
        private val streamsManager: StreamsManager,
        private val stateManager: StateManager
    ) {
        fun make(nShardsPerKey: Int) = TestContext(
            writer = DestinationMessageQueueWriter(
                catalog = catalog,
                messageQueue = messageQueue.make(nShardsPerKey),
                streamsManager = streamsManager,
                stateManager = stateManager
            ),
            streamsManager = streamsManager,
            stateManager = stateManager
        )
    }

    /**
     * Scenarios:
     *   * nShards: esp 1 versus >1
     *   * record versus state versus other
     *   * record versus end-of-stream
     *     - record routed to shard
     *     - end-of-stream routed to all shards
     *   * global versus stream state
     *     - global index sent to stream manager even when sharded
     *   * other ignored
     *
     *   - new queue writer each time
     *   - same catalog is probably fine?
     *   - new streams manager and state manager each time
     */
    class DestinationMessageQueueWriterTestArguments: ArgumentsProvider {
        override fun provideArguments(context: ExtensionContext?): Stream<out Arguments> {
            TODO("Not yet implemented")
        }
    }

    @ParameterizedTest
    @ArgumentsSource(DestinationMessageQueueWriterTestArguments::class)
    fun testWritingRecord(nShards: Int, messages: List<Pair<DestinationMessage, Long>>) = runTest {
        val ctx = testContextFactory.make(nShards)
        messages.forEach { (message, size) ->
            ctx.writer.publish(message, size)
        }

        // Validate that all global and stream state messages ended up in the state manager
        // with the appropriate indexes

        // Validate that all records were counted and routed to the appropriate shard
    }
}
