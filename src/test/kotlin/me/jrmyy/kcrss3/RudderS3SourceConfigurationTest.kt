package me.jrmyy.kcrss3

import org.apache.kafka.common.config.ConfigException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class RudderS3SourceConfigurationTest {

    private val fullConf: Map<String, String> = mapOf(
        "s3.bucket.name" to "bucket",
        "topic" to "topic",
        "s3.region" to "us-east-1",
        "s3.bucket.prefixes" to "abc/def,123/456",
        "s3.bucket.max-files" to "344",
        "s3.poll.interval.ms" to "6789",
        "aws.access.key.id" to "accId",
        "aws.secret.access.key" to "secKey",
    )

    private val testConfig = RudderS3SourceConfiguration(fullConf)

    @Test
    fun initWillFailBecauseMissingBucketName() {
        val properties = fullConf.toMutableMap()
        properties.remove("s3.bucket.name")
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Missing required configuration \"s3.bucket.name\" which has no default value.",
            exception.message,
        )
    }

    @Test
    fun initWillFailBecauseEmptyBucketName() {
        val properties = fullConf.toMutableMap()
        properties["s3.bucket.name"] = ""
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value  for configuration s3.bucket.name: String must be non-empty",
            exception.message,
        )
    }

    @Test
    fun getS3Bucket() {
        assertEquals("bucket", testConfig.getS3Bucket())
    }

    @Test
    fun getS3Region() {
        assertEquals("us-east-1", testConfig.getS3Region())
    }

    @Test
    fun getS3RegionFallbackToDefault() {
        val properties = fullConf.toMutableMap()
        properties.remove("s3.region")
        val config = RudderS3SourceConfiguration(properties)
        assertEquals("eu-west-1", config.getS3Region())
    }

    @Test
    fun getS3RegionInvalid() {
        val properties = fullConf.toMutableMap()
        properties["s3.region"] = "oo-west-1"
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value oo-west-1 for configuration s3.region: AWS region is invalid",
            exception.message,
        )
    }

    @Test
    fun getS3FolderPrefixes() {
        assertEquals(listOf("abc/def", "123/456"), testConfig.getS3FolderPrefixes())
    }

    @Test
    fun getS3FolderPrefixesFallbackToDefault() {
        val properties = fullConf.toMutableMap()
        properties.remove("s3.bucket.prefixes")
        val config = RudderS3SourceConfiguration(properties)
        assertEquals(listOf(""), config.getS3FolderPrefixes())
    }

    @Test
    fun getS3FolderPrefixesNoneProvided() {
        val properties = fullConf.toMutableMap()
        properties["s3.bucket.prefixes"] = ""
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value [] for configuration s3.bucket.prefixes: lower than minimum " +
                "list size of [1]",
            exception.message,
        )
    }

    @Test
    fun getMaxFilesToFetch() {
        assertEquals(344, testConfig.getMaxFilesToFetch())
    }

    @Test
    fun getMaxFilesToFetchFallbackToDefault() {
        val properties = fullConf.toMutableMap()
        properties.remove("s3.bucket.max-files")
        val config = RudderS3SourceConfiguration(properties)
        assertEquals(1, config.getMaxFilesToFetch())
    }

    @Test
    fun getMaxFilesToFetchTooMuch() {
        val properties = fullConf.toMutableMap()
        properties["s3.bucket.max-files"] = "1001"
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value 1001 for configuration s3.bucket.max-files: " +
                "Value must be no more than 1000",
            exception.message,
        )
    }

    @Test
    fun getPollIntervalM() {
        assertEquals(6789, testConfig.getPollIntervalMs())
    }

    @Test
    fun getPollIntervalMsFallbackToDefault() {
        val properties = fullConf.toMutableMap()
        properties.remove("s3.poll.interval.ms")
        val config = RudderS3SourceConfiguration(properties)
        assertEquals(30_000, config.getPollIntervalMs())
    }

    @Test
    fun getPollIntervalMsNegativeInt() {
        val properties = fullConf.toMutableMap()
        properties["s3.poll.interval.ms"] = "-1"
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value -1 for configuration s3.poll.interval.ms: Value must be at least 0",
            exception.message,
        )
    }

    @Test
    fun getAWSAccessKey() {
        assertEquals("accId", testConfig.getAWSAccessKey())
    }

    @Test
    fun initWillFailBecauseMissingAccessKey() {
        val properties = fullConf.toMutableMap()
        properties.remove("aws.access.key.id")
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Missing required configuration \"aws.access.key.id\" which has no default value.",
            exception.message,
        )
    }

    @Test
    fun initWillFailBecauseEmptyAccessKey() {
        val properties = fullConf.toMutableMap()
        properties["aws.access.key.id"] = ""
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value  for configuration aws.access.key.id: String must be non-empty",
            exception.message,
        )
    }

    @Test
    fun getAWSSecretKey() {
        assertEquals("secKey", testConfig.getAWSSecretKey())
    }

    @Test
    fun initWillFailBecauseMissingSecretKey() {
        val properties = fullConf.toMutableMap()
        properties.remove("aws.secret.access.key")
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Missing required configuration \"aws.secret.access.key\" which has no default value.",
            exception.message,
        )
    }

    @Test
    fun initWillFailBecauseEmptySecretKey() {
        val properties = fullConf.toMutableMap()
        properties["aws.secret.access.key"] = ""
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value  for configuration aws.secret.access.key: String must be non-empty",
            exception.message,
        )
    }

    @Test
    fun getTopic() {
        assertEquals("topic", testConfig.getTopic())
    }

    @Test
    fun initWillFailBecauseMissingTopic() {
        val properties = fullConf.toMutableMap()
        properties.remove("topic")
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Missing required configuration \"topic\" which has no default value.",
            exception.message,
        )
    }

    @Test
    fun initWillFailBecauseEmptyTopic() {
        val properties = fullConf.toMutableMap()
        properties["topic"] = ""
        val exception = assertFailsWith<ConfigException> {
            RudderS3SourceConfiguration(properties)
        }
        assertEquals(
            "Invalid value  for configuration topic: String must be non-empty",
            exception.message,
        )
    }

    @Test
    fun getConfigDef() {
        val configDef = RudderS3SourceConfiguration.getConfigDef()
        assertEquals(
            configDef.configKeys().keys,
            mutableSetOf(
                "topic",
                "s3.bucket.name",
                "s3.region",
                "s3.bucket.prefixes",
                "s3.bucket.max-files",
                "s3.poll.interval.ms",
                "aws.access.key.id",
                "aws.secret.access.key",
            ),
        )
        assertEquals(
            configDef.defaultValues(),
            mapOf(
                "s3.region" to "eu-west-1",
                "s3.bucket.prefixes" to listOf(""),
                "s3.bucket.max-files" to 1,
                "s3.poll.interval.ms" to 30_000,
            ),
        )
    }
}
