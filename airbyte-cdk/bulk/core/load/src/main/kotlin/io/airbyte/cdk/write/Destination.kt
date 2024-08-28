package io.airbyte.cdk.write

import io.airbyte.cdk.command.DestinationStream
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

interface Destination {
    // Called once before anything else
    fun setup() {}

    // Return a StreamLoader for the given stream
    fun getStreamLoader(stream: DestinationStream): StreamLoader

    // Called once at the end of the job
    fun teardown(succeeded: Boolean = true) {}
}

@Singleton
@Secondary
class DefaultDestination(
    private val streamLoaderFactory: StreamLoaderFactory
): Destination {
    override fun getStreamLoader(stream: DestinationStream): StreamLoader {
        return streamLoaderFactory.make(stream)
    }
}
