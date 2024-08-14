package io.airbyte.cdk.command

import io.airbyte.cdk.message.BatchEnvelope
import io.airbyte.cdk.message.MessageQueue
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import jakarta.inject.Provider
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow


class DestinationStream(
    val descriptor: Descriptor
) {
    data class Descriptor(
        val namespace: String,
        val name: String
    )

    override fun hashCode(): Int {
        return descriptor.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return other is DestinationStream && descriptor == other.descriptor
    }

    override fun toString(): String {
        return "DestinationStream(descriptor=$descriptor)"
    }
}


@Singleton
class DestinationStreamFactory {
    fun make(stream: ConfiguredAirbyteStream): DestinationStream {
        return DestinationStream(
            descriptor = DestinationStream.Descriptor(
                namespace = stream.stream.namespace,
                name = stream.stream.name
            )
        )
    }
}
