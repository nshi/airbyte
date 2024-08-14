package io.airbyte.cdk.message

import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.state.StateManager
import io.airbyte.cdk.state.StreamsManager
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Prototype
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject

@MicronautTest
class DestinationMessageQueueWriterTest {
    @Prototype
    class 

    @Factory
    class QueueWriterFactory(
        private val catalog: DestinationCatalog,
        private val messageQueue: DestinationMessageQueue,
        private val streamsManager: StreamsManager,
        private val stateManager: StateManager
    ) {

        @Prototype
        fun make() = DestinationMessageQueueWriter(
            catalog = catalog,
            messageQueue = messageQueue,
            streamsManager = streamsManager,
            stateManager = stateManager
        )
    }

    fun testWritingRecord() {

    }
}
