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


class ScrawlerModule extends SimpleModule("scrawler", new Version(1, 0, 0, null, "com.sonar", "scrawler")) {
    override def setupModule(context: SetupContext) {
        context.addDeserializationProblemHandler(new DeserializationProblemHandlerImpl)
    }
}

class DeserializationProblemHandlerImpl extends DeserializationProblemHandler with Logging {
    override def handleUnknownProperty(ctxt: DeserializationContext, jp: JsonParser, deserializer: JsonDeserializer[_], beanOrClass: Any, propertyName: String) = {
        error("Problem deserializing property: " + propertyName)
        //        ctxt.getParser.skipChildren()
        true
    }
}

class ScrawlerObjectMapper extends ObjectMapper {

    this.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
    this.getDeserializationConfig.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    this.getSerializationConfig.without(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    this.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
    this.addMixInAnnotations(classOf[ServiceProfileDTO], classOf[IgnoreUnknownMixin])
    this.addMixInAnnotations(classOf[UserEducation], classOf[IgnoreUnknownMixin])
    this.addMixInAnnotations(classOf[Checkin], classOf[IgnoreUnknownMixin])
    this.addMixInAnnotations(classOf[UserEmployment], classOf[UserEmploymentMixin])
    this.addMixInAnnotations(classOf[ServiceProfileDTO], classOf[IgnoreUnknownMixin])

}

object ScrawlerObjectMapper {

    var initialized = false
    val module = new ScrawlerModule()
    val objectMapper = new ScrawlerObjectMapper()

    init()

    def init() {
        // functionality includes ability to register serializers, deserializers, add mix-in annotations etc:
        module.setMixInAnnotation(classOf[ServiceProfileDTO], classOf[IgnoreUnknownMixin])
        module.setMixInAnnotation(classOf[UserEducation], classOf[IgnoreUnknownMixin])
        module.setMixInAnnotation(classOf[Checkin], classOf[IgnoreUnknownMixin])
        module.setMixInAnnotation(classOf[UserEmployment], classOf[UserEmploymentMixin])
        module.setMixInAnnotation(classOf[ServiceProfileDTO], classOf[IgnoreUnknownMixin])
        // and the magic happens here when we register module with mapper:
        objectMapper.registerModule(module)
        initialized = true
    }


    def mapper(): ObjectMapper = {
        if (!initialized) {
            init()
        }
        objectMapper
    }

    def parseJson[T](jsonStringOption: Option[String], classType:Class[T]): Option[T] = {
        for (jsonString <- jsonStringOption if jsonString.contains("""{""")) yield {
            mapper().readValue(jsonString, classType)
        }
    }

}

@JsonIgnoreProperties(ignoreUnknown = true)
abstract class IgnoreUnknownMixin {

    @scala.reflect.BeanProperty
    @JsonProperty("is_current")
    var current: Boolean = _

}

@JsonIgnoreProperties(ignoreUnknown = true)
abstract class UserEmploymentMixin {

    @scala.reflect.BeanProperty
    @JsonProperty("is_current")
    var current: Boolean = _

}

