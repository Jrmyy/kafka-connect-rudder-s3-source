package me.jrmyy.kcrss3

import me.jrmyy.kcrss3.kafka.KafkaConfigValidator
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef

class RudderS3SourceConfiguration(
    properties: Map<String, String>
) {

    private val parsedConfig: AbstractConfig = AbstractConfig(CONFIG_DEF, properties)

    fun getS3FolderPrefixes(): List<String> = parsedConfig.getList(S3_BUCKET_PREFIXES)

    fun getS3Bucket(): String = parsedConfig.getString(S3_BUCKET_NAME)

    fun getS3Region(): String = parsedConfig.getString(S3_REGION)

    fun getMaxFilesToFetch(): Int = parsedConfig.getInt(S3_BUCKET_MAX_FILES)

    fun getTopic(): String = parsedConfig.getString(TOPIC_NAME)

    fun getPollIntervalMs(): Int = parsedConfig.getInt(POLL_INTERVAL_MS)

    fun getAWSAccessKey(): String = parsedConfig.getString(AWS_ACCESS_KEY_ID)

    fun getAWSSecretKey(): String = parsedConfig.getString(AWS_SECRET_ACCESS_KEY)

    companion object {

        private const val TOPIC_NAME = "topic"
        private const val S3_BUCKET_NAME = "s3.bucket.name"
        private const val S3_REGION = "s3.region"
        private const val S3_BUCKET_PREFIXES = "s3.bucket.prefixes"
        private const val S3_BUCKET_MAX_FILES = "s3.bucket.max-files"
        private const val POLL_INTERVAL_MS = "s3.poll.interval.ms"
        private const val AWS_ACCESS_KEY_ID = "aws.access.key.id"
        private const val AWS_SECRET_ACCESS_KEY = "aws.secret.access.key"

        private val CONFIG_DEF = ConfigDef()
            .define(
                S3_BUCKET_NAME,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "The name of the bucket"
            )
            .define(
                S3_REGION,
                ConfigDef.Type.STRING,
                "eu-west-1",
                KafkaConfigValidator.AWSRegion(),
                ConfigDef.Importance.HIGH,
                "The region in which is located the bucket"
            )
            .define(
                S3_BUCKET_PREFIXES,
                ConfigDef.Type.LIST,
                listOf(""),
                KafkaConfigValidator.ListSize.atLeast(1),
                ConfigDef.Importance.HIGH,
                "Since the source connector is relying on the `startAfter` parameter of the s3 API, we " +
                    "will fetch new files based on their key name. Therefore, the source connector makes it " +
                    "mandatory, for each folder indicated in this parameter, to have new files coming " +
                    "alphabetically after the previous files. This is possible is they are prefixed by the " +
                    "timestamp for instance."
            )
            .define(
                S3_BUCKET_MAX_FILES,
                ConfigDef.Type.INT,
                1,
                ConfigDef.Range.between(1, 1000),
                ConfigDef.Importance.MEDIUM,
                "The number of files to retrieve every time the S3 API is called."
            )
            .define(
                POLL_INTERVAL_MS,
                ConfigDef.Type.INT,
                30_000,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.MEDIUM,
                "The number of milliseconds to wait between 2 polls (default: 30 seconds)"
            )
            .define(
                AWS_ACCESS_KEY_ID,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "The access key of the user in charge of reading the files from S3. It should also have " +
                    "the right to download a file."
            )
            .define(
                AWS_SECRET_ACCESS_KEY,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "The secret key of the user in charge of reading the files from S3. It should also have " +
                    "the right to download a file."
            )
            .define(
                TOPIC_NAME,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "The topic to publish data to"
            )

        fun getConfigDef(): ConfigDef = CONFIG_DEF
    }
}
