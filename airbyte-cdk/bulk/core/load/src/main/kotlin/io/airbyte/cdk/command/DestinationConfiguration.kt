package io.airbyte.cdk.command

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@ConfigurationProperties("destination.config")
interface DestinationConfiguration: Configuration {
    /**
     * Micronaut factory which glues [ConfigurationJsonObjectSupplier] and
     * [SourceConfigurationFactory] together to produce a [SourceConfiguration] singleton.
     */
    @Factory
    private class MicronautFactory {
        @Singleton
        fun <I : ConfigurationJsonObjectBase> sourceConfig(
            pojoSupplier: ConfigurationJsonObjectSupplier<I>,
            factory: DestinationConfigurationFactory<I, out DestinationConfiguration>,
        ): DestinationConfiguration = factory.make(pojoSupplier.get())
    }
}
