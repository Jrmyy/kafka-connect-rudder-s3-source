package me.jrmyy.kcrss3

import ch.qos.logback.classic.Level
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import me.jrmyy.kcrss3.exceptions.TooManyMaxTasksException
import org.apache.kafka.common.config.ConfigDef
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class RudderS3SourceConnectorTest : BaseTestWithLogging("me.jrmyy.kcrss3.RudderS3SourceConnector") {

    private val connector = RudderS3SourceConnector()

    @Test
    fun version() {
        mockkObject(RudderS3SourceVersion.Companion)
        every { RudderS3SourceVersion.Companion.getVersion() } returns "1.0.0"
        assertEquals("1.0.0", connector.version())
    }

    @Test
    fun start() {
        connector.start(mapOf("a" to "b"))
        assertEquals(3, listAppender.list.size)
        assertEquals(Level.INFO, listAppender.list[0].level)
        assertEquals("Source started", listAppender.list[0].message)
        assertEquals(Level.DEBUG, listAppender.list[1].level)
        assertEquals("Source configuration:", listAppender.list[1].message)
        assertEquals(Level.DEBUG, listAppender.list[2].level)
        assertEquals("* a: b", listAppender.list[2].message)
    }

    @Test
    fun taskClass() {
        assertEquals("RudderS3SourceTask", connector.taskClass().simpleName)
    }

    @Test
    fun taskConfigs() {
        connector.start(mapOf("a" to "b"))
        val res = connector.taskConfigs(1)
        assertEquals(1, res.size)
        assertEquals(mapOf("a" to "b"), res.first())
    }

    @Test
    fun taskConfigsTooMany() {
        connector.start(mapOf("a" to "b"))
        val exception = assertFailsWith<TooManyMaxTasksException> {
            connector.taskConfigs(2)
        }
        assertEquals("This connector can only be configured with tasks.max = 1.", exception.message)
    }

    @Test
    fun stop() {
        connector.stop()
        assertEquals(1, listAppender.list.size)
        assertEquals(Level.INFO, listAppender.list[0].level)
        assertEquals("Source stopped", listAppender.list[0].message)
    }

    @Test
    fun config() {
        val mock = mockk<ConfigDef>()
        mockkObject(RudderS3SourceConfiguration.Companion)
        every { RudderS3SourceConfiguration.Companion.getConfigDef() } returns mock
        assertEquals(mock, connector.config())
    }
}
