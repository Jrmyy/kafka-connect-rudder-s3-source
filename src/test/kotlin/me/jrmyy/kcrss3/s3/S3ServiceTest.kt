package me.jrmyy.kcrss3.s3

import aws.sdk.kotlin.runtime.ClientException
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.*
import aws.smithy.kotlin.runtime.content.ByteStream
import ch.qos.logback.classic.Level
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import me.jrmyy.kcrss3.BaseTestWithLogging
import java.nio.file.Files
import kotlin.io.path.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

@OptIn(ExperimentalCoroutinesApi::class)
internal class S3ServiceTest : BaseTestWithLogging("me.jrmyy.kcrss3.s3.S3Service") {

    private val client = S3Service(
        awsAccessKey = "accKey",
        awsSecretKey = "secKey",
        s3Region = "eu-west-1",
        s3Bucket = "bucket",
        maxFiles = 12,
    )

    @Test
    fun getNewKeysSinceReturnsSomeKeys() {
        testGetNewKeysReturns(
            ListObjectsV2Response {
                contents = listOf(
                    Object { key = "path/to/folder/abc" },
                    Object { key = "" },
                )
            },
            listOf("path/to/folder/abc"),
        )
    }

    @Test
    fun getNewKeysSinceReturnsEmptyBecauseNoValidKeys() {
        testGetNewKeysReturns(
            ListObjectsV2Response {
                contents = listOf(
                    Object { key = null },
                    Object { key = "" },
                )
            },
            listOf(),
        )
    }

    @Test
    fun getNewKeysSinceReturnsEmptyBecauseNoContents() {
        testGetNewKeysReturns(
            ListObjectsV2Response {
                contents = null
            },
            listOf(),
        )
    }

    @Test
    fun getNewKeysSinceReturnsEmptyBecauseS3Exception() {
        testGetNewKeysThrows(S3Exception())
    }

    @Test
    fun getNewKeysSinceReturnsEmptyBecauseClientException() {
        testGetNewKeysThrows(ClientException())
    }

    @Test
    fun getNewKeysSinceRaisesBecauseUnknownException() {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            coEvery { mockClient.listObjectsV2(any()) } throws RuntimeException()

            assertFailsWith<RuntimeException> {
                client.getNewKeysSince("path/to/folder", "")
            }
        }
    }

    private fun testGetNewKeysReturns(response: ListObjectsV2Response, expected: List<String>) {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            coEvery { mockClient.listObjectsV2(any()) } returns response

            val files = client.getNewKeysSince("path/to/folder", "")

            assertEquals(expected, files)
            coVerify {
                mockClient.listObjectsV2(
                    ListObjectsV2Request {
                        bucket = "bucket"
                        startAfter = ""
                        prefix = "path/to/folder"
                        maxKeys = 12
                    },
                )
            }
        }
    }

    private fun testGetNewKeysThrows(exception: Exception) {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            coEvery { mockClient.listObjectsV2(any()) } throws exception

            val files = client.getNewKeysSince("path/to/folder", "")

            assertEquals(listOf(), files)
            coVerify {
                mockClient.listObjectsV2(
                    ListObjectsV2Request {
                        bucket = "bucket"
                        startAfter = ""
                        prefix = "path/to/folder"
                        maxKeys = 12
                    },
                )
            }
        }
    }

    @Test
    fun storeS3FileLocallyReturnsNullBecauseBodyIsNull() {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            coEvery {
                mockClient.getObject(
                    any(),
                    any<suspend (GetObjectResponse) -> ByteStream?>(),
                )
            } returns null

            assertNull(client.storeS3FileLocally("path/to/folder/key"))
        }
    }

    @Test
    fun storeS3FileLocallyReturnsNullBecauseKeyNotFound() {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            coEvery {
                mockClient.getObject(
                    any(),
                    any<suspend (GetObjectResponse) -> ByteStream?>(),
                )
            } throws NoSuchKey.invoke { }

            assertNull(client.storeS3FileLocally("path/to/folder/key"))
        }
    }

    @Test
    fun storeS3FileLocallyReturnsNullBecauseClientException() {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            coEvery {
                mockClient.getObject(
                    any(),
                    any<suspend (GetObjectResponse) -> ByteStream?>(),
                )
            } throws ClientException()

            assertNull(client.storeS3FileLocally("path/to/folder/key"))
        }
    }

    @Test
    fun storeS3FileLocallyRaisesBecauseUnknownException() {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            coEvery {
                mockClient.getObject(
                    any(),
                    any<suspend (GetObjectResponse) -> ByteStream?>(),
                )
            } throws RuntimeException()

            assertFailsWith<RuntimeException> {
                client.storeS3FileLocally("path/to/folder/key")
            }
        }
    }

    @Test
    fun storeS3FileLocallyWillWork() {
        runTest {
            mockkObject(S3ClientBuilder)
            val mockClient = mockk<S3Client>(relaxed = true)
            every {
                S3ClientBuilder.build("accKey", "secKey", "eu-west-1")
            } returns mockClient

            val mockByteStream = mockk<ByteStream.Buffer>(relaxed = true)
            coEvery {
                mockClient.getObject(
                    any(),
                    any<suspend (GetObjectResponse) -> ByteStream?>(),
                )
            } returns mockByteStream

            mockkStatic(Files::createDirectories)

            val path = client.storeS3FileLocally("path/to/folder/key")

            assertEquals(path, "/tmp/path/to/folder/key")
            coVerify {
                mockClient.getObject(
                    match<GetObjectRequest> {
                        it.bucket == "bucket" && it.key == "path/to/folder/key"
                    },
                    any<suspend (GetObjectResponse) -> ByteStream?>(),
                )
            }
            verify {
                Files.createDirectories(Path("/tmp/path/to/folder"))
            }

            assertEquals(1, listAppender.list.size)
            assertEquals(Level.DEBUG, listAppender.list[0].level)
            assertEquals(
                "Successfully read `path/to/folder/key` and stored it at `/tmp/path/to/folder/key`",
                listAppender.list[0].message,
            )
        }
    }
}
