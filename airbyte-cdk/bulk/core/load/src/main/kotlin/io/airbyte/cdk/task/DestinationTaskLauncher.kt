package io.airbyte.cdk.task

import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.message.BatchEnvelope
import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Provider
import jakarta.inject.Singleton


class DestinationTaskLauncher(
    config: WriteConfiguration,
    private val catalog: DestinationCatalog,
    override val taskQueue: TaskQueue,

    private val setupTaskFactory: SetupTaskFactory,
    private val openStreamTaskFactory: OpenStreamTaskFactory,
    private val accumulateRecordsTaskFactory: AccumulateRecordsTaskFactory,
    private val processBatchTaskFactory: ProcessBatchTaskFactory,
    private val closeStreamTaskFactory: CloseStreamTaskFactory,
    private val teardownTaskFactory: TeardownTaskFactory
): TaskLauncher {
    private val log = KotlinLogging.logger {}

    private val nAccumulators = config.getNumAccumulatorsPerStream(catalog.streams.size)

    override suspend fun start() {
        log.info { "Enqueueing startup task" }
        taskQueue.enqueue(setupTaskFactory.make(this))
    }

    suspend fun enqueueOpenStreamTasks() {
        catalog.streams.forEach {
            log.info { "Enqueueing open stream task for $it" }
            taskQueue.enqueue(openStreamTaskFactory.make(this, it))
        }
    }

    suspend fun enqueueAccumulateRecordsTasks(streamLoader: StreamLoader) {
        for (shard in 0 until nAccumulators) {
            log.info { "Enqueueing accumulate records task for ${streamLoader.stream}, shard $shard" }
            val task = accumulateRecordsTaskFactory.make(this, streamLoader, shard)
            taskQueue.enqueue(task)
        }
    }

    suspend fun reenqueueAccumulateRecordsTask(streamLoader: StreamLoader, shard: Int, nextState: AccumulateRecordsTask.State) {
        log.info { "Re-enqueueing accumulate records task for ${streamLoader.stream}, shard $shard, nextState: $nextState" }
        taskQueue.enqueue(accumulateRecordsTaskFactory.make(this, streamLoader, shard, nextState))
    }

    suspend fun enqueueProcessBatchTask(streamLoader: StreamLoader, batch: BatchEnvelope) {
        log.info { "Enqueueing process batch task for ${streamLoader.stream}, batch ${batch.batch}" }
        taskQueue.enqueue(processBatchTaskFactory.make(this, streamLoader, batch))
    }

    suspend fun enqueueCloseStreamTask(streamLoader: StreamLoader) {
        log.info { "Enqueueing close stream task for ${streamLoader.stream}" }
        taskQueue.enqueue(closeStreamTaskFactory.make(this, streamLoader))
    }

    suspend fun enqueueTeardownTask() {
        log.info { "Enqueueing teardown task" }
        taskQueue.enqueue(teardownTaskFactory.make(this))
    }
}

@Factory
class DestinationTaskLauncherFactory(
    private val config: WriteConfiguration,
    private val catalog: DestinationCatalog,
    private val taskQueue: TaskQueue,

    private val setupTaskFactory: SetupTaskFactory,
    private val openStreamTaskFactory: OpenStreamTaskFactory,
    private val accumulateRecordsTaskFactory: AccumulateRecordsTaskFactory,
    private val processBatchTaskFactory: ProcessBatchTaskFactory,
    private val closeStreamTaskFactory: CloseStreamTaskFactory,
    private val teardownTaskFactory: TeardownTaskFactory
): Provider<DestinationTaskLauncher> {
    @Singleton
    @Secondary
    override fun get(): DestinationTaskLauncher {
        return DestinationTaskLauncher(
            config,
            catalog,
            taskQueue,

            setupTaskFactory,
            openStreamTaskFactory,
            accumulateRecordsTaskFactory,
            processBatchTaskFactory,
            closeStreamTaskFactory,
            teardownTaskFactory,
        )
    }
}

