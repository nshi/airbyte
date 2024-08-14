package io.airbyte.cdk.state

import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.message.AirbyteStateMessageFactory
import io.airbyte.cdk.message.DestinationStateMessage
import io.airbyte.cdk.output.OutputConsumer
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.clhm.ConcurrentLinkedHashMap
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

@Singleton
class StateManager(
    private val catalog: DestinationCatalog,
    private val streamsManager: StreamsManager,
    private val stateMessageFactory: AirbyteStateMessageFactory,
    private val outputConsumer: OutputConsumer
) {
    private val log = KotlinLogging.logger {}

    private val stateIsGlobal: AtomicReference<Boolean?> = AtomicReference(null)

    private val streamStates: ConcurrentHashMap<DestinationStream,
        LinkedHashMap<Long, DestinationStateMessage>> = ConcurrentHashMap()

    private val globalStates: ConcurrentLinkedHashMap<Long, DestinationStateMessage> =
        ConcurrentLinkedHashMap.Builder<Long, DestinationStateMessage>()
            .initialCapacity(1000)
            .maximumWeightedCapacity(1000)
            .build()

    fun addState(stream: DestinationStream,
                 stateMessage: DestinationStateMessage,
                 index: Long,
                 isGlobal: Boolean
    ) {
        if (stateIsGlobal.getAndSet(isGlobal) != isGlobal) {
            throw IllegalStateException("Global state cannot be mixed with non-global state")
        }

        // Grab the latest index for this stream to calculate count
        if (isGlobal) {
            globalStates[index] = stateMessage
            log.info { "Added global state at index: $index" }
        } else {
            val streamStates = streamStates.getOrPut(stream) { LinkedHashMap() }
            streamStates[index] = stateMessage
            log.info { "Added state for stream: $stream at index: $index" }
        }

        // TODO: Make this non-blocking
        flushStates()
    }

    private fun flushStates() {
        /*
            Iterate over the states in order, evicting each that passes
            the persistence check. If a state is not persisted, then
            we can break the loop since the states are ordered. For global
            states, all streams must be persisted up to the checkpoint.
         */
        when (stateIsGlobal.get()) {
            null -> log.info { "No states to flush" }
            true -> flushGlobalStates()
            false -> flushStreamStates()
        }
    }

    private fun flushGlobalStates() {
        for (index in globalStates.keys) {
            if (catalog.streams.all {
                    streamsManager.getManager(it).areRecordsPersistedUntil(index)
                }) {
                val stateMessage = globalStates.remove(index)
                    ?: throw IllegalStateException("State not found for index: $index")
                log.info { "Flushing global state for index: $index" }
                val outMessage = stateMessageFactory.fromDestinationStateMessage(stateMessage)
                outputConsumer.accept(outMessage)
            } else {
                break
            }
        }
    }

    private fun flushStreamStates() {
        for (stream in catalog.streams) {
            val manager = streamsManager.getManager(stream)
            val streamStates = streamStates[stream] ?: return
            for (index in streamStates.keys) {
                if (manager.areRecordsPersistedUntil(index)) {
                    val stateMessage = streamStates.remove(index)
                        ?: throw IllegalStateException("State not found for index: $index")
                    log.info { "Flushing state for stream: $stream at index: $index" }
                    val outMessage = stateMessageFactory.fromDestinationStateMessage(stateMessage)
                    outputConsumer.accept(outMessage)
                } else {
                    break
                }
            }
        }
    }
}
