package me.jrmyy.kcrss3

import me.jrmyy.kcrss3.exceptions.TooManyMaxTasksException
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

class RudderS3SourceConnector : SourceConnector() {
    private val properties: MutableMap<String, String> = mutableMapOf()

    override fun version(): String {
        return RudderS3SourceVersion.getVersion()
    }

    override fun start(props: Map<String, String>) {
        logger.info("Source started")
        logger.debug("Source configuration:")
        props.forEach { (key, value) -> logger.debug("* $key: $value") }
        properties.clear()
        properties.putAll(props)
    }

    override fun taskClass(): Class<out Task> = RudderS3SourceTask::class.java

    override fun taskConfigs(maxTasks: Int): List<Map<String, String?>> {
        if (maxTasks != 1) {
            throw TooManyMaxTasksException(
                "This connector can only be configured with tasks.max = 1.",
            )
        }
        return listOf(properties)
    }

    override fun stop() {
        logger.info("Source stopped")
    }

    override fun config(): ConfigDef = RudderS3SourceConfiguration.getConfigDef()

    companion object {
        private val logger: KLogger = KotlinLogging.logger(RudderS3SourceConnector::class.java.name)
    }
}
