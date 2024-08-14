package io.airbyte.cdk.message

import io.airbyte.cdk.util.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import jakarta.inject.Singleton

interface Deserializer<T> {
    fun deserialize(message: String): T
}

@Singleton
class DefaultDestinationMessageDeserializer(
    private val messageFactory: DestinationMessageFactory
) : Deserializer<DestinationMessage> {
    override fun deserialize(message: String): DestinationMessage {
        try {
            val node = Jsons.readTree(message)
            val airbyteMessage = Jsons.treeToValue(node, AirbyteMessage::class.java)
            return messageFactory.fromAirbyteMessage(airbyteMessage)
        } catch (e: Exception) {
            throw RuntimeException("Failed to deserialize AirbyteMessage: $message", e)
        }
    }
}
