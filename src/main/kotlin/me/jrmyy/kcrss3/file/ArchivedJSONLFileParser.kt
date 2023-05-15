package me.jrmyy.kcrss3.file

import mu.KotlinLogging
import java.io.File
import java.io.IOException
import java.util.zip.GZIPInputStream

class ArchivedJSONLFileParser : FileParser {

    override fun parse(filePath: String): List<String> {
        return try {
            GZIPInputStream(File(filePath).inputStream())
                .bufferedReader()
                .useLines { it.toList() }
        } catch (exc: IOException) {
            logger.catching(exc)
            listOf()
        }
    }

    companion object {
        private val logger = KotlinLogging.logger(ArchivedJSONLFileParser::class.java.name)
    }
}
