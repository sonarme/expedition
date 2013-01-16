package com.sonar.expedition.scrawler.json

import com.fasterxml.jackson.databind._


object ScrawlerObjectMapper extends ObjectMapper {

    this.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
    this.getDeserializationConfig.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    this.getSerializationConfig.without(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    this.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)

    def parseJson[T](jsonString: String)(implicit m: Manifest[T]): Option[T] = {
        if (jsonString.startsWith("{\""))
            Option(readValue(jsonString, m.erasure.asInstanceOf[Class[T]]))
        else None
    }

    def parseJsonBytes[T](json: Array[Byte])(implicit m: Manifest[T]): Option[T] = {
        val value = readValue(json, m.erasure.asInstanceOf[Class[T]])
        if (value == null) throw new RuntimeException("value was null")
        Some(value)
    }
}
