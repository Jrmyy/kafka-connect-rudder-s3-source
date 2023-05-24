package me.jrmyy.kcrss3

import aws.smithy.kotlin.runtime.util.length
import ch.qos.logback.classic.Level
import io.mockk.*
import org.apache.kafka.common.utils.Utils.sleep
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class RudderS3SourceTaskTest : BaseTestWithLogging("me.jrmyy.kcrss3.RudderS3SourceTask") {

    private val task = RudderS3SourceTask()

    @Test
    fun version() {
        mockkObject(RudderS3SourceVersion)
        every { RudderS3SourceVersion.getVersion() } returns "1.0.0"
        assertEquals("1.0.0", task.version())
    }

    @Test
    fun start() {
        mockkObject(RudderS3SourceService.Companion)

        val mockContext = mockk<SourceTaskContext>()
        val mockOffsetStorageReader = mockk<OffsetStorageReader>()
        every { mockContext.offsetStorageReader() } returns mockOffsetStorageReader

        task.initialize(mockContext)
        task.start(
            mapOf(
                "topic" to "topic",
                "s3.bucket.name" to "bucket",
                "aws.access.key.id" to "accKey",
                "aws.secret.access.key" to "secKey",
            ),
        )

        assertEquals(1, listAppender.list.size)
        assertEquals(Level.INFO, listAppender.list[0].level)
        assertEquals("Task starting", listAppender.list[0].message)

        verify {
            RudderS3SourceService.Companion.build(
                any<RudderS3SourceConfiguration>(),
                mockOffsetStorageReader,
            )
        }
    }

    @Test
    fun stop() {
        task.stop()
        assertEquals(1, listAppender.list.size)
        assertEquals(Level.INFO, listAppender.list[0].level)
        assertEquals("Task stopped", listAppender.list[0].message)
    }

    @Test
    fun poll() {
        mockkStatic(::sleep)
        every { sleep(any()) } answers { }

        val mockContext = mockk<SourceTaskContext>()
        val mockOffsetStorageReader = mockk<OffsetStorageReader>()
        every { mockContext.offsetStorageReader() } returns mockOffsetStorageReader

        mockkObject(RudderS3SourceService.Companion)
        val mockService = mockk<RudderS3SourceService>()
        every {
            RudderS3SourceService.Companion.build(any(), mockOffsetStorageReader)
        } returns mockService

        every { mockService.generateRecords() } returns listOf()

        task.initialize(mockContext)
        task.start(
            mapOf(
                "topic" to "topic",
                "s3.bucket.name" to "bucket",
                "aws.access.key.id" to "accKey",
                "aws.secret.access.key" to "secKey",
            ),
        )
        task.poll()

        verify {
            mockService.generateRecords()
        }
        verify(exactly = 0) {
            sleep(any())
        }
    }

    @Test
    fun pollWillSleep() {
        mockkStatic(::sleep)
        every { sleep(any()) } answers { }

        val mockContext = mockk<SourceTaskContext>()
        val mockOffsetStorageReader = mockk<OffsetStorageReader>()
        every { mockContext.offsetStorageReader() } returns mockOffsetStorageReader

        mockkObject(RudderS3SourceService.Companion)
        val mockService = mockk<RudderS3SourceService>()
        every {
            RudderS3SourceService.Companion.build(any(), mockOffsetStorageReader)
        } returns mockService

        every { mockService.generateRecords() } returns listOf()

        task.initialize(mockContext)
        task.start(
            mapOf(
                "topic" to "topic",
                "s3.bucket.name" to "bucket",
                "aws.access.key.id" to "accKey",
                "aws.secret.access.key" to "secKey",
            ),
        )
        task.poll()
        task.poll()

        verify(exactly = 2) {
            mockService.generateRecords()
        }
        verify(exactly = 1) {
            sleep(any())
        }

        val debugs = listAppender.list.filter {
            it.level == Level.DEBUG && it.message.startsWith("Waiting ")
        }
        assertEquals(1, debugs.length)
        assertTrue("Waiting (\\d+) ms to poll.".toRegex().matches(debugs.first().message))
    }
}
