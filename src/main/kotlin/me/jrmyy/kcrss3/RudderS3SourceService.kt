package me.jrmyy.kcrss3

import aws.smithy.kotlin.runtime.util.length
import kotlinx.coroutines.runBlocking
import me.jrmyy.kcrss3.file.ArchivedJSONLFileParser
import me.jrmyy.kcrss3.file.FileParser
import me.jrmyy.kcrss3.s3.S3Service
import me.jrmyy.kcrss3.utils.TimeHelper
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader
import java.io.File

class RudderS3SourceService(
    private val folders: List<String>,
    private val topic: String,
    private val offsetStorageReader: OffsetStorageReader,
    private val s3Service: S3Service,
    private val parser: FileParser = ArchivedJSONLFileParser(),
) {

    fun generateRecords(): List<SourceRecord> =
        folders.flatMap { folder ->
            val offset = offsetStorageReader.offset(offsetKey(folder))
            val lastFile = if (offset != null) {
                val lastFileOffsetValue = offset.getOrDefault(LAST_FILE_FIELD, null)

                if (lastFileOffsetValue != null && lastFileOffsetValue !is String) {
                    throw ConnectException("Offset position is of incorrect type.")
                }

                val lastFileOffset = lastFileOffsetValue as String?

                if (!lastFileOffset.isNullOrBlank()) {
                    logger.debug(
                        "Found a last file offset `$lastFileOffset`, will fetch the records " +
                            "from here.",
                    )
                    lastFileOffset
                } else {
                    logger.debug("Offset is null or blank, starting from the beginning.")
                    ""
                }
            } else {
                logger.debug("No offset, starting from the beginning.")
                ""
            }

            val offsetKey = offsetKey(folder)

            val newFiles = runBlocking { s3Service.getNewKeysSince(folder, lastFile) }
            newFiles.flatMapIndexed { fileIdx, file ->
                val localPath = runBlocking { s3Service.storeS3FileLocally(file) }
                if (localPath == null) {
                    listOf()
                } else {
                    val prevFileName = if (fileIdx > 0) newFiles[fileIdx - 1] else lastFile
                    val records = parser.parse(localPath)
                    // Until we reach the last line of the file, the offset value should be the previous file because
                    // in case of failure, we will have to download the file again and produce the data in the topic.
                    // Once we are dealing with the last line of the file, we can set the offset value to the current
                    // file. This ensures us at-least-once delivery and in case of failure will not respect
                    // exactly-once delivery.
                    val sourceRecords = records.dropLast(1).mapIndexed { lineIdx, line ->
                        SourceRecord(
                            offsetKey,
                            offsetValue(prevFileName),
                            topic,
                            null,
                            Schema.STRING_SCHEMA,
                            "$file-$lineIdx",
                            Schema.STRING_SCHEMA,
                            line,
                            TimeHelper.getNow(),
                        )
                    } + listOf(
                        SourceRecord(
                            offsetKey,
                            offsetValue(file),
                            topic,
                            null,
                            Schema.STRING_SCHEMA,
                            "$file-${records.length - 1}",
                            Schema.STRING_SCHEMA,
                            records.last(),
                            TimeHelper.getNow(),
                        ),
                    )
                    File(localPath).delete()
                    sourceRecords
                }
            }
        }

    private fun offsetKey(prefix: String): Map<String, String> {
        return mapOf(PREFIX_FIELD to prefix)
    }

    private fun offsetValue(lastFile: String): Map<String, String> {
        return mapOf(LAST_FILE_FIELD to lastFile)
    }

    companion object {

        private val logger: KLogger = KotlinLogging.logger(RudderS3SourceService::class.java.name)

        private const val PREFIX_FIELD = "prefix"
        private const val LAST_FILE_FIELD = "last_file"

        fun build(
            configuration: RudderS3SourceConfiguration,
            offsetStorageReader: OffsetStorageReader,
        ): RudderS3SourceService {
            val s3Service = S3Service(
                awsAccessKey = configuration.getAWSAccessKey(),
                awsSecretKey = configuration.getAWSSecretKey(),
                s3Region = configuration.getS3Region(),
                s3Bucket = configuration.getS3Bucket(),
                maxFiles = configuration.getMaxFilesToFetch(),
            )
            return RudderS3SourceService(
                s3Service = s3Service,
                offsetStorageReader = offsetStorageReader,
                topic = configuration.getTopic(),
                folders = configuration.getS3FolderPrefixes(),
            )
        }
    }
}
