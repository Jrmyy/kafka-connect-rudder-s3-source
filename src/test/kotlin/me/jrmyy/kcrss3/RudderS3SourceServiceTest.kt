package me.jrmyy.kcrss3

import ch.qos.logback.classic.Level
import io.mockk.*
import me.jrmyy.kcrss3.file.FileParser
import me.jrmyy.kcrss3.s3.S3Service
import me.jrmyy.kcrss3.utils.TimeHelper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.AfterEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RudderS3SourceServiceTest : BaseTestWithLogging("me.jrmyy.kcrss3.RudderS3SourceService") {

    private val mockOffsetStorageReader = mockk<OffsetStorageReader>()
    private val mockS3Service = mockk<S3Service>()
    private val mockParser = mockk<FileParser>()

    private val service = RudderS3SourceService(
        listOf("abc", "def"),
        "topic",
        mockOffsetStorageReader,
        mockS3Service,
        mockParser,
    )

    @AfterEach
    fun afterEach() {
        clearAllMocks()
    }

    @Test
    fun generateRecordsWillRaiseConnectionException() {
        every {
            mockOffsetStorageReader.offset(
                mapOf("prefix" to "abc"),
            )
        } returns mapOf("last_file" to 3)
        val exception = assertFailsWith<ConnectException> {
            service.generateRecords()
        }
        assertEquals("Offset position is of incorrect type.", exception.message)
    }

    @Test
    fun generateRecordsWithNoOffset() {
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "abc"))
        } returns null
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "def"))
        } returns null
        coEvery {
            mockS3Service.getNewKeysSince(any(), "")
        } returns listOf()

        val records = service.generateRecords()

        assertEquals(listOf(), records)

        assertEquals(2, listAppender.list.size)
        assertEquals(Level.DEBUG, listAppender.list[0].level)
        assertEquals("No offset, starting from the beginning.", listAppender.list[0].message)
        assertEquals(Level.DEBUG, listAppender.list[1].level)
        assertEquals("No offset, starting from the beginning.", listAppender.list[1].message)
    }

    @Test
    fun generateRecordsWithNullOrBlankOffset() {
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "abc"))
        } returns mapOf("last_file" to null)
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "def"))
        } returns mapOf("last_file" to "")
        coEvery {
            mockS3Service.getNewKeysSince(any(), "")
        } returns listOf()

        val records = service.generateRecords()

        assertEquals(listOf(), records)

        assertEquals(2, listAppender.list.size)
        assertEquals(Level.DEBUG, listAppender.list[0].level)
        assertEquals(
            "Offset is null or blank, starting from the beginning.",
            listAppender.list[0].message,
        )
        assertEquals(Level.DEBUG, listAppender.list[1].level)
        assertEquals(
            "Offset is null or blank, starting from the beginning.",
            listAppender.list[1].message,
        )
    }

    @Test
    fun generateRecordsWithOffsets() {
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "abc"))
        } returns mapOf("last_file" to "key-abc")
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "def"))
        } returns mapOf("last_file" to "key-def")
        coEvery {
            mockS3Service.getNewKeysSince(any(), any())
        } returns listOf()

        val records = service.generateRecords()

        assertEquals(listOf(), records)

        assertEquals(2, listAppender.list.size)
        assertEquals(Level.DEBUG, listAppender.list[0].level)
        assertEquals(
            "Found a last file offset `key-abc`, will fetch the records from here.",
            listAppender.list[0].message,
        )
        assertEquals(Level.DEBUG, listAppender.list[1].level)
        assertEquals(
            "Found a last file offset `key-def`, will fetch the records from here.",
            listAppender.list[1].message,
        )
    }

    @Test
    fun generateRecordsWithNonExistingFile() {
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "abc"))
        } returns null
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "def"))
        } returns null
        coEvery {
            mockS3Service.getNewKeysSince(any(), "")
        } returns listOf(
            "file",
        )
        coEvery {
            mockS3Service.storeS3FileLocally(any())
        } returns null

        val records = service.generateRecords()

        assertEquals(listOf(), records)
    }

    @Test
    fun generateRecordsWillReturnWithCorrectOffsets() {
        mockkObject(TimeHelper)
        every { TimeHelper.getNow() } returns 1234L
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "abc"))
        } returns mapOf("last_file" to "key-abc")
        every {
            mockOffsetStorageReader.offset(mapOf("prefix" to "def"))
        } returns mapOf("last_file" to "key-def")

        coEvery {
            mockS3Service.getNewKeysSince(any(), "key-abc")
        } returns listOf("new-key-abc-1", "new-key-abc-2")
        coEvery {
            mockS3Service.getNewKeysSince(any(), "key-def")
        } returns listOf("new-key-def")

        coEvery {
            mockS3Service.storeS3FileLocally("new-key-abc-1")
        } returns "new-key-abc-1"
        coEvery {
            mockS3Service.storeS3FileLocally("new-key-abc-2")
        } returns "new-key-abc-2"
        coEvery {
            mockS3Service.storeS3FileLocally("new-key-def")
        } returns "new-key-def"

        every {
            mockParser.parse("new-key-abc-1")
        } returns listOf("rec_abc_1", "rec_abc_2")
        every {
            mockParser.parse("new-key-abc-2")
        } returns listOf("rec_abc_3", "rec_abc_4")
        every {
            mockParser.parse("new-key-def")
        } returns listOf("rec_def_1", "rec_def_2", "rec_def_3")

        val records = service.generateRecords()

        assertEquals(7, records.size)

        assertEquals(
            SourceRecord(
                mapOf("prefix" to "abc"),
                mapOf("last_file" to "key-abc"),
                "topic",
                null,
                Schema.STRING_SCHEMA,
                "new-key-abc-1-0",
                Schema.STRING_SCHEMA,
                "rec_abc_1",
                1234L,
            ),
            records.first(),
        )

        assertEquals(
            SourceRecord(
                mapOf("prefix" to "abc"),
                mapOf("last_file" to "new-key-abc-1"),
                "topic",
                null,
                Schema.STRING_SCHEMA,
                "new-key-abc-1-1",
                Schema.STRING_SCHEMA,
                "rec_abc_2",
                1234L,
            ),
            records[1],
        )

        assertEquals(
            SourceRecord(
                mapOf("prefix" to "abc"),
                mapOf("last_file" to "new-key-abc-1"),
                "topic",
                null,
                Schema.STRING_SCHEMA,
                "new-key-abc-2-0",
                Schema.STRING_SCHEMA,
                "rec_abc_3",
                1234L,
            ),
            records[2],
        )

        assertEquals(
            SourceRecord(
                mapOf("prefix" to "abc"),
                mapOf("last_file" to "new-key-abc-2"),
                "topic",
                null,
                Schema.STRING_SCHEMA,
                "new-key-abc-2-1",
                Schema.STRING_SCHEMA,
                "rec_abc_4",
                1234L,
            ),
            records[3],
        )

        assertEquals(
            SourceRecord(
                mapOf("prefix" to "def"),
                mapOf("last_file" to "key-def"),
                "topic",
                null,
                Schema.STRING_SCHEMA,
                "new-key-def-0",
                Schema.STRING_SCHEMA,
                "rec_def_1",
                1234L,
            ),
            records[4],
        )

        assertEquals(
            SourceRecord(
                mapOf("prefix" to "def"),
                mapOf("last_file" to "key-def"),
                "topic",
                null,
                Schema.STRING_SCHEMA,
                "new-key-def-1",
                Schema.STRING_SCHEMA,
                "rec_def_2",
                1234L,
            ),
            records[5],
        )

        assertEquals(
            SourceRecord(
                mapOf("prefix" to "def"),
                mapOf("last_file" to "new-key-def"),
                "topic",
                null,
                Schema.STRING_SCHEMA,
                "new-key-def-2",
                Schema.STRING_SCHEMA,
                "rec_def_3",
                1234L,
            ),
            records[6],
        )
    }
}
