package io.airbyte.cdk.write

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.message.Batch
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

interface StreamLoader {
    val stream: DestinationStream

    fun open() {}
    fun getRecordAccumulator(shard: Int): RecordAccumulator
    fun processBatch(batch: Batch): Batch = batch.withState(Batch.State.COMPLETE)
    fun close() {}
}


class DefaultStreamLoader(
    override val stream: DestinationStream,
    private val recordAccumulatorFactory: RecordAccumulatorFactory
) : StreamLoader {
    val log = KotlinLogging.logger {}

    override fun getRecordAccumulator(shard: Int): RecordAccumulator {
        log.info { "DefaultStreamLoader.getRecordAccumulator(stream=$stream, shard=$shard)" }
        return recordAccumulatorFactory.make(this, shard)
    }
}

interface StreamLoaderFactory {
    fun make(destination: Destination, stream: DestinationStream): StreamLoader
}

@Singleton
@Secondary
class DefaultStreamLoaderFactory(
    private val recordAccumulatorFactory: RecordAccumulatorFactory,
) : StreamLoaderFactory {
    override fun make(destination: Destination, stream: DestinationStream): StreamLoader {
        return DefaultStreamLoader(stream, recordAccumulatorFactory)
    }
}
