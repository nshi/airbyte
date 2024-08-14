package io.airbyte.cdk.task

import io.airbyte.cdk.write.Destination
import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.WriteConfiguration
import io.airbyte.cdk.state.StreamsManager
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Provider
import jakarta.inject.Singleton
import kotlinx.coroutines.delay


class TeardownTask(
    private val config: WriteConfiguration,
    private val destination: Destination,
    private val streamsManager: StreamsManager,
    private val taskLauncher: DestinationTaskLauncher
): Task {
    val log = KotlinLogging.logger {}

    override val concurrency = Task.Concurrency("teardown", maxRuns = 1)

    override suspend fun execute() {
        var i = 0
        while (streamsManager.openStreamCount() > 0) {
            val waitMs = config.noTaskAvailableWaitTimeMs
            delay(waitMs)
            if (i++ % 10 == 0) {
                log.info { "Waiting for ${streamsManager.openStreamCount()} streams to close after ${waitMs * i}ms..." }
            }
        }

        destination.teardown()
        taskLauncher.stop()
    }
}


@Singleton
@Secondary
class TeardownTaskFactory(
    private val config: WriteConfiguration,
    private val destination: Destination,
    private val streamsManager: StreamsManager,
) {
    fun make(taskLauncher: DestinationTaskLauncher): TeardownTask {
        return TeardownTask(config, destination, streamsManager, taskLauncher)
    }
}
