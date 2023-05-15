package me.jrmyy.kcrss3

class RudderS3SourceVersion {
    companion object {
        fun getVersion(): String = BuildConfig.APP_VERSION
    }
}
