package com.sonar.expedition.scrawler.json

import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.map.{DeserializationConfig, PropertyNamingStrategy, ObjectMapper}

class CustomObjectMapper extends ObjectMapper {
    setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
    setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL)
    disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES)
}


object JacksonObjectMapper {

  val objectMapper:ObjectMapper = new CustomObjectMapper()

}