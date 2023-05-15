package me.jrmyy.kcrss3.s3

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

@OptIn(ExperimentalCoroutinesApi::class)
internal class AWSStaticCredentialsProviderTest {

    private val provider = AWSStaticCredentialsProvider("accKey", "secKey")

    @Test
    fun resolve() {
        runTest {
            val credentials = provider.resolve()
            assertEquals(credentials.accessKeyId, "accKey")
            assertEquals(credentials.secretAccessKey, "secKey")
        }
    }
}
