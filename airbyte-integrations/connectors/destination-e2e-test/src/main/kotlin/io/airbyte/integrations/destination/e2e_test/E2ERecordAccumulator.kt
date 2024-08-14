package io.airbyte.integrations.destination.e2e_test

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.DestinationRecord
import io.airbyte.cdk.write.RecordAccumulator
import io.airbyte.cdk.write.RecordAccumulatorFactory
import io.airbyte.cdk.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicInteger


interface E2ERecordAccumulator: RecordAccumulator {
    override fun flush(endOfStream: Boolean): Batch {
        return Batch(state = Batch.State.PERSISTED)
    }
}

@Factory
class E2ERecordAccumulatorFactoryFactory {
    @Singleton
    fun make(config: E2EDestinationConfiguration): RecordAccumulatorFactory {
        return when (config.testDestination) {
            is LoggingDestination -> LoggingAccumulatorFactory(config.testDestination.loggingConfig)
            is SilentDestination -> SilentAccumulatorFactory()
            is ThrottledDestination -> ThrottledAccumulatorFactory(config.testDestination.millisPerRecord)
            is FailingDestination -> FailingAccumulatorFactory(config.testDestination.numMessages)
        }
    }
}

class LoggingAccumulatorFactory(
    private val loggingConfig: LoggingConfig
): RecordAccumulatorFactory {
    class LoggingAccumulator(
        private val stream: DestinationStream,
        private val shard: Int,
        private val maxEntryCount: Int,
        private val logEvery: Int,
        private val sampleRate: Double
    ): E2ERecordAccumulator {
        private val log = KotlinLogging.logger {}

        companion object {
            private val recordCount = AtomicInteger(0)
            private val logCount = AtomicInteger(0)
        }

        override fun accept(record: DestinationRecord): Batch {
            recordCount.getAndIncrement().let { recordCount ->
                if (recordCount % logEvery == 0) {
                    if (Math.random() < sampleRate) {
                        logCount.getAndIncrement().let { logCount ->
                            if (logCount < maxEntryCount) {
                                log.info { "Logging Destination(stream=${stream.descriptor}, shard=$shard, recordIndex=$recordCount, logEntry=$logCount/$maxEntryCount): $record" }
                            }
                        }
                    }
                }
            }

            return Batch(state = Batch.State.ACCUMULATING)
        }
    }

    override fun make(streamLoader: StreamLoader, shard: Int) =
        when (loggingConfig) {
            is FirstNEntriesConfig -> LoggingAccumulator(streamLoader.stream, shard, loggingConfig.maxEntryCount, 1, 1.0)
            is EveryNthEntryConfig -> LoggingAccumulator(streamLoader.stream, shard, loggingConfig.maxEntryCount, loggingConfig.nthEntryToLog, 1.0)
            is RandomSamplingConfig -> LoggingAccumulator(streamLoader.stream, shard, loggingConfig.maxEntryCount, 1, loggingConfig.samplingRatio)
        }
}

class SilentAccumulatorFactory: RecordAccumulatorFactory {
    override fun make(streamLoader: StreamLoader, shard: Int): RecordAccumulator {
        return object : E2ERecordAccumulator {
            override fun accept(record: DestinationRecord): Batch? = null
        }
    }
}

class ThrottledAccumulatorFactory(
    val millisPerRecord: Long
): RecordAccumulatorFactory {
    override fun make(streamLoader: StreamLoader, shard: Int): RecordAccumulator {
        return object : E2ERecordAccumulator {
            override fun accept(record: DestinationRecord): Batch? {
                Thread.sleep(millisPerRecord)
                return null
            }
        }
    }
}

class FailingAccumulatorFactory(
    private val numMessages: Int
): RecordAccumulatorFactory {
    class FailingAccumulator(
        private val stream: DestinationStream,
        private val shard: Int,
        private val numMessages: Int
    ): E2ERecordAccumulator {
        private val log = KotlinLogging.logger {}

        companion object {
            private val messageCount = AtomicInteger(0)
        }

        override fun accept(record: DestinationRecord): Batch? {
            messageCount.getAndIncrement().let { messageCount ->
                if (messageCount > numMessages) {
                    val message = "Failing Destination(stream=${stream.descriptor}, shard=$shard, numMessages=$numMessages: failing at $record"
                    log.info { message }
                    throw RuntimeException(message)
                }
            }

            return null
        }
    }

    override fun make(streamLoader: StreamLoader, shard: Int): RecordAccumulator {
        return FailingAccumulator(streamLoader.stream, shard, numMessages)
    }
}
