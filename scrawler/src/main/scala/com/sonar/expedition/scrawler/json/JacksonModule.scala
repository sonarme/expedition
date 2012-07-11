package com.sonar.expedition.scrawler.json

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.{JsonParser, Version}
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind._
import com.sonar.dossier.dto.{Checkin, UserEducation, UserEmployment, ServiceProfileDTO}
import deser.DeserializationProblemHandler
import grizzled.slf4j.Logging

class ScrawlerModule extends SimpleModule("scrawler", new Version(1, 0, 1, null)) {
    override def setupModule(context: SetupContext) {
        context.addDeserializationProblemHandler(new DeserializationProblemHandlerImpl)
    }
}

class DeserializationProblemHandlerImpl extends DeserializationProblemHandler with Logging{
    override def handleUnknownProperty(ctxt: DeserializationContext, jp: JsonParser, deserializer: JsonDeserializer[_], beanOrClass: Any, propertyName: String) = {
        error("Problem deserializing property: " + propertyName)
//        ctxt.getParser.skipChildren()
        true
    }
}

class ScrawlerObjectMapper extends ObjectMapper {

    setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
    this.getDeserializationConfig.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    this.getSerializationConfig.without(SerializationFeature.FAIL_ON_EMPTY_BEANS)
}

object ScrawlerObjectMapper {

    val objectMapper = new ScrawlerObjectMapper()
    val module = new ScrawlerModule();
    // functionality includes ability to register serializers, deserializers, add mix-in annotations etc:
    module.setMixInAnnotation(classOf[ServiceProfileDTO], classOf[IgnoreUnknownMixin])
    module.setMixInAnnotation(classOf[UserEmployment], classOf[IgnoreUnknownMixin])
    module.setMixInAnnotation(classOf[UserEducation], classOf[IgnoreUnknownMixin])
    module.setMixInAnnotation(classOf[Checkin], classOf[IgnoreUnknownMixin])
    // and the magic happens here when we register module with mapper:
    objectMapper.registerModule(module);

    def mapper(): ObjectMapper = {
        objectMapper
    }

}

@JsonIgnoreProperties(ignoreUnknown = true)
abstract class IgnoreUnknownMixin

