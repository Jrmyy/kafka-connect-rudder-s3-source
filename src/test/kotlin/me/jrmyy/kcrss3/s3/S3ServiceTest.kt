package me.jrmyy.kcrss3.s3

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Response
import aws.sdk.kotlin.services.s3.model.Object
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import me.jrmyy.kcrss3.BaseTestWithLogging
import kotlin.test.Test
import kotlin.test.assertEquals

@OptIn(ExperimentalCoroutinesApi::class)
internal class S3ServiceTest : BaseTestWithLogging("me.jrmyy.kcrss3.S3Service") {

    private val client = S3Service(
        awsAccessKey = "accKey",
        awsSecretKey = "secKey",
        s3Region = "eu-west-1",
        s3Bucket = "bucket",
        maxFiles = 12
    )

    @Test
    fun getNewFilesSinceWithReturnsSomeKeys() {
        testGetNewFiles(
            ListObjectsV2Response {
                contents = listOf(
                    Object { key = "path/to/folder/abc" },
                    Object { key = "" }
                )
            },
            listOf("path/to/folder/abc")
        )
    }

    @Test
    fun getNewFilesSinceWithReturnsEmptyBecauseNoValidKeys() {
        testGetNewFiles(
            ListObjectsV2Response {
                contents = listOf(
                    Object { key = null },
                    Object { key = "" }
                )
            },
            listOf()
        )
    }

    @Test
    fun getNewFilesSinceWithReturnsEmptyBecauseNoContents() {
        testGetNewFiles(
            ListObjectsV2Response {
                contents = null
            },
            listOf()
        )
    }

    private fun testGetNewFiles(response: ListObjectsV2Response, expected: List<String>) {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = spyk<S3Client>()
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient
            coEvery { mockClient.listObjectsV2(any()) } returns response
            val files = client.getNewKeysSince("path/to/folder", "")
            assertEquals(expected, files)
            verify {
                mockClient invoke "listObjectsV2" withArguments listOf(
                    ListObjectsV2Request {
                        bucket = "bucket"
                        startAfter = ""
                        prefix = "path/to/folder"
                        maxKeys = 12
                    }
                )
            }
        }
    }

    @Test
    fun storeS3FileLocally() {
    }
}
