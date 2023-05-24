package me.jrmyy.kcrss3.s3

import aws.sdk.kotlin.runtime.ClientException
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.sdk.kotlin.services.s3.model.S3Exception
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.writeToFile
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mu.KLogger
import mu.KotlinLogging
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

class S3Service(
    private val awsAccessKey: String,
    private val awsSecretKey: String,
    private val s3Region: String,
    private val s3Bucket: String,
    private val maxFiles: Int,
    private val localFolder: String = "/tmp",
) {

    suspend fun getNewKeysSince(s3Folder: String, lastSeenFile: String?): List<String> {
        val request = ListObjectsV2Request {
            bucket = s3Bucket
            startAfter = lastSeenFile
            prefix = s3Folder
            maxKeys = maxFiles
        }
        val client = S3ClientBuilder.build(awsAccessKey, awsSecretKey, s3Region)
        return try {
            client.use { s3 ->
                val response = s3.listObjectsV2(request)
                response.contents?.map { it.key.orEmpty() }.orEmpty().filter { it != "" }
            }
        } catch (ex: Exception) {
            when (ex) {
                is S3Exception, is ClientException -> listOf()
                else -> throw ex
            }
        }
    }

    private suspend fun getFileContent(s3Key: String): ByteStream? {
        val request = GetObjectRequest {
            bucket = s3Bucket
            key = s3Key
        }
        val client = S3ClientBuilder.build(awsAccessKey, awsSecretKey, s3Region)
        return try {
            client.use { s3 ->
                s3.getObject(request) { resp -> resp.body }
            }
        } catch (ex: Exception) {
            when (ex) {
                is S3Exception, is ClientException -> null
                else -> throw ex
            }
        }
    }

    suspend fun storeS3FileLocally(s3Key: String): String? {
        val content = getFileContent(s3Key) ?: return null
        val parts = "$localFolder/$s3Key".split("/").toMutableList()
        val file = parts.removeLast()
        val folder = parts.joinToString("/")
        val path = "$folder/$file"
        withContext(Dispatchers.IO) {
            Files.createDirectories(Paths.get(folder))
        }
        content.writeToFile(File(path))
        logger.debug("Successfully read `$s3Key` and stored it at `$path`")
        return path
    }

    companion object {
        private val logger: KLogger = KotlinLogging.logger(S3Service::class.java.name)
    }
}
