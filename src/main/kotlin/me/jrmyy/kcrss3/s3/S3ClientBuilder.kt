package me.jrmyy.kcrss3.s3

import aws.sdk.kotlin.services.s3.S3Client
import aws.smithy.kotlin.runtime.auth.awscredentials.CachedCredentialsProvider

object S3ClientBuilder {
    fun build(awsAccessKey: String, awsSecretKey: String, s3Region: String): S3Client =
        S3Client {
            region = s3Region
            credentialsProvider = CachedCredentialsProvider(
                AWSStaticCredentialsProvider(awsAccessKey, awsSecretKey),
            )
        }
}
