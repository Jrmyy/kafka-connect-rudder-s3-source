package me.jrmyy.kcrss3.file

import ch.qos.logback.classic.Level
import me.jrmyy.kcrss3.BaseTestWithLogging
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

internal class ArchivedJSONLFileParserTest : BaseTestWithLogging(
    "me.jrmyy.kcrss3.file.ArchivedJSONLFileParser"
) {

    private val parser = ArchivedJSONLFileParser()

    @Test
    fun parseOK() {
        assertEquals(
            listOf("{\"a\":\"b\"}", "{\"c\":\"d\"}"),
            parser.parse(
                File("src/test/resources/fakedata.jsonl.gz").absolutePath
            )
        )
    }

    @Test
    fun parseKOBecauseIOException() {
        assertEquals(
            listOf(),
            parser.parse(
                File("src/test/resources/nonexistent.jsonl.gz").absolutePath
            )
        )
        assertEquals(1, listAppender.list.size)
        assertEquals(Level.ERROR, listAppender.list.first().level)
        assertEquals("java.io.FileNotFoundException", listAppender.list.first().throwableProxy.className)
    }
}
