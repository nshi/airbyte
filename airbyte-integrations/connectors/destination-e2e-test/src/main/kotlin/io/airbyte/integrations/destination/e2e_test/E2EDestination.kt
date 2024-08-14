package io.airbyte.integrations.destination.e2e_test

import io.airbyte.cdk.AirbyteDestinationRunner
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.write.Destination
import io.airbyte.cdk.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton
import java.io.FileInputStream
import java.io.InputStream


class E2EDestination {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            AirbyteDestinationRunner.run(*args)
        }
    }
}

@Factory
class InputStreamFactory {
    @Singleton
    @Primary
    fun make(): InputStream {
        return FileInputStream("test.jsonl")
    }
}
