package me.jrmyy.kcrss3.kafka

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException

object KafkaConfigValidator {
    class ListSize private constructor(private val minSize: Int) : ConfigDef.Validator {
        override fun ensureValid(name: String?, value: Any?) {
            val values = value as List<*>
            if (values.size < minSize) {
                throw ConfigException(name, value, "lower than minimum list size of [$minSize]")
            }
        }

        companion object {
            fun atLeast(minSize: Int): ListSize =
                ListSize(minSize)
        }
    }

    class AWSRegion : ConfigDef.Validator {
        override fun ensureValid(name: String?, value: Any?) {
            val region = value as String
            if (!REGEX.matches(region)) {
                throw ConfigException(name, value, "AWS region is invalid")
            }
        }

        companion object {
            private val REGEX =
                "(us(-gov)?|ap|ca|cn|eu|sa)-(central|(north|south)?(east|west)?)-\\d".toRegex()
        }
    }
}
