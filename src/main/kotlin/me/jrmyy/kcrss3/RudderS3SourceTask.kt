package me.jrmyy.kcrss3

import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.common.utils.Utils.sleep
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

class RudderS3SourceTask : SourceTask() {

    private lateinit var rudderS3SourceS3Service: RudderS3SourceService

    private var pollIntervalMs: Int = 0
    private var lastRun: Long = 0

    override fun version(): String {
        return RudderS3SourceVersion.getVersion()
    }

    override fun start(props: Map<String, String>) {
        logger.info("Task starting")

        val configuration = RudderS3SourceConfiguration(props)

        pollIntervalMs = configuration.getPollIntervalMs()
        rudderS3SourceS3Service = RudderS3SourceService.build(configuration, context.offsetStorageReader())
    }

    override fun stop() {
        logger.info("Task stopped")
    }

    override fun poll(): List<SourceRecord> {
        val nextUpdate = lastRun + pollIntervalMs
        val untilNext = nextUpdate - System.currentTimeMillis()
        if (untilNext > 0) {
            logger.debug("Waiting $untilNext ms to poll.")
            sleep(untilNext)
        }
        val records = rudderS3SourceS3Service.generateRecords()
        lastRun = System.currentTimeMillis()
        return records
    }

    companion object {
        private val logger: KLogger = KotlinLogging.logger(RudderS3SourceTask::class.java.name)
    }
}
