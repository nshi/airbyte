package io.airbyte.cdk.task

import io.airbyte.cdk.state.StreamsManager
import io.airbyte.cdk.write.Destination
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicBoolean


class TeardownTask(
    private val destination: Destination,
    private val streamsManager: StreamsManager,
    private val taskLauncher: DestinationTaskLauncher
): Task {
    val log = KotlinLogging.logger {}

    companion object {
        val exactlyOnce = AtomicBoolean(false)
    }

    override suspend fun execute() {
        /** Guard against running this more than once */
        if (exactlyOnce.getAndSet(true)) {
            return
        }

        /** Ensure we don't run until all streams have completed */
        streamsManager.awaitAllStreamsComplete()

        destination.teardown()
        taskLauncher.stop()
    }
}


@Singleton
@Secondary
class TeardownTaskFactory(
    private val destination: Destination,
    private val streamsManager: StreamsManager,
) {
    fun make(taskLauncher: DestinationTaskLauncher): TeardownTask {
        return TeardownTask(destination, streamsManager, taskLauncher)
    }
}
