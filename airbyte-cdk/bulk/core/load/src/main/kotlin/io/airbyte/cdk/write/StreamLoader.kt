package io.airbyte.cdk.write

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.DestinationRecord
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

interface StreamLoader {
    val stream: DestinationStream

    fun open() {}
    fun processRecords(records: Iterator<DestinationRecord>, totalSizeBytes: Long): Batch
    fun processBatch(batch: Batch): Batch = object: Batch { override val state = Batch.State.COMPLETE }
    fun close() {}
}

class DefaultStreamLoader(
    override val stream: DestinationStream,
) : StreamLoader {
    val log = KotlinLogging.logger {}

    override fun processRecords(records: Iterator<DestinationRecord>, totalSizeBytes: Long): Batch {
        TODO("Default implementation adds airbyte metadata, maybe flattens, no-op maps, and converts to destination format")
    }
}

interface StreamLoaderFactory {
    fun make(stream: DestinationStream): StreamLoader
}

@Singleton
@Secondary
class DefaultStreamLoaderFactory(
) : StreamLoaderFactory {
    override fun make(stream: DestinationStream): StreamLoader {
        TODO("See above")
    }
}
