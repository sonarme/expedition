package com.sonar.expedition.scrawler.json

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.{JsonParser, Version}
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind._
import com.sonar.dossier.dto.{Checkin, UserEducation, UserEmployment, ServiceProfileDTO}
import deser.DeserializationProblemHandler
import grizzled.slf4j.Logging
import org.codehaus.jackson.annotate.JsonProperty
import scala.collection.JavaConversions._


object ScrawlerObjectMapper extends ObjectMapper {

    this.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
    this.getDeserializationConfig.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    this.getSerializationConfig.without(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    this.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)

    def parseJson[T](jsonString: String)(implicit m: Manifest[T]): Option[T] = {
        for (jsonString <- Option(jsonString) if jsonString.contains( """{""");
             value <- try {
                 Some(readValue(jsonString, m.erasure.asInstanceOf[Class[T]]))
             }
             catch {
                 case e: Exception => None
             }
        ) yield value
    }

    def parseJsonBytes[T](json: Array[Byte])(implicit m: Manifest[T]): Option[T] = {
        val value = readValue(json, m.erasure.asInstanceOf[Class[T]])
        if (value == null) throw new RuntimeException("value was null")
        Some(value)
    }
}
