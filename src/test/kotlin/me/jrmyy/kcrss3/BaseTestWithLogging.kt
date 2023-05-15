package me.jrmyy.kcrss3

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory

open class BaseTestWithLogging(private val loggerName: String) {
    internal val listAppender = ListAppender<ILoggingEvent>()

    @BeforeEach
    fun setup() {
        val logger = LoggerFactory.getLogger(loggerName) as Logger
        logger.addAppender(listAppender)
        listAppender.start()
    }

    @AfterEach
    fun teardown() {
        listAppender.stop()
        val logger = LoggerFactory.getLogger(loggerName) as Logger
        logger.detachAppender(listAppender)
    }
}
