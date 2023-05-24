import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    distribution
    kotlin("jvm") version "1.8.20"
    id("org.jmailen.kotlinter") version "3.14.0"
    id("org.jetbrains.kotlinx.kover") version "0.7.0-Beta"
    id("com.github.gmazzo.buildconfig") version "4.0.4"
}

group = "com.jobteaser"
version = "1.0.0"

val kafkaVersion = "3.4.0"

repositories {
    mavenCentral()
}

buildConfig {
    useKotlinOutput { internalVisibility = true }
    packageName("me.jrmyy.kcrss3")
    className("BuildConfig")
    buildConfigField("String", "APP_VERSION", provider { "\"${project.version}\"" })
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("org.apache.kafka:connect-api:$kafkaVersion")

    implementation("aws.sdk.kotlin:s3:0.22.0-beta")

    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.4.7")

    testImplementation(kotlin("test"))
    testImplementation("io.mockk:mockk:1.13.5")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.6.4")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}

tasks {
    distZip.get().dependsOn(jar.get())
    distTar.get().dependsOn(jar.get())
}

tasks.check {
    dependsOn("installKotlinterPrePushHook")
}

tasks.lintKotlinMain {
    exclude("me/jrmyy/kcrss3/BuildConfig.kt")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

distributions {
    main {
        contents {
            into("") {
                from(configurations.runtimeClasspath)
                exclude("**/connect-api-*.jar", "**/kafka-clients-*.jar")
                from("$buildDir/libs")
                dirMode = 755
                fileMode = 644
            }
        }
    }
}
