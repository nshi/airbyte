package io.airbyte.cdk.write

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.DestinationRecord
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton


interface RecordAccumulatorFactory {
    fun make(streamLoader: StreamLoader, shard: Int): RecordAccumulator
}

interface RecordAccumulator {
    fun accept(record: DestinationRecord): Batch?
    fun flush(endOfStream: Boolean): Batch
}


@Singleton
@Secondary
class DefaultRecordAccumulatorFactory: RecordAccumulatorFactory {
    override fun make(streamLoader: StreamLoader, shard: Int): RecordAccumulator {
        throw NotImplementedError("A minimal destination implementation requires at least a RecordAccumulatorFactory singleton")
    }
}
