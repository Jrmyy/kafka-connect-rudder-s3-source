package me.jrmyy.kcrss3.s3

import aws.smithy.kotlin.runtime.auth.awscredentials.Credentials
import aws.smithy.kotlin.runtime.auth.awscredentials.CredentialsProvider
import aws.smithy.kotlin.runtime.util.Attributes

class AWSStaticCredentialsProvider(
    private val accessKey: String,
    private val secretKey: String,
) : CredentialsProvider {
    override suspend fun resolve(attributes: Attributes): Credentials = Credentials(
        accessKeyId = accessKey,
        secretAccessKey = secretKey,
    )
}
