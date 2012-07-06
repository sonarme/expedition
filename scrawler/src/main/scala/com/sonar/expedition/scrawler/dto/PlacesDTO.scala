package com.sonar.expedition.scrawler.dto

import reflect.BeanProperty
import org.codehaus.jackson.annotate.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesDTO(@BeanProperty
                     var geometry: PlacesGeometryDTO = PlacesGeometryDTO(),
                     @BeanProperty
                     var `type`: String = null,
                     @BeanProperty
                     var id: String = null,
                     @BeanProperty
                     var properties: PlacesPropertiesDTO = PlacesPropertiesDTO()
                     ) {
    def this() = this(null, null)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesGeometryDTO(@BeanProperty
                             var `type`: String = null,
                             @BeanProperty
                             var coordinates: Array[Double] = null) {

    def this() = this(null)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesPropertiesDTO(@BeanProperty
                               var province: String = null,
                               @BeanProperty
                               var city: String = null,
                               @BeanProperty
                               var name: String = null,
                               @BeanProperty
                               var tags: java.util.List[String] = null,
                               @BeanProperty
                               var country: String = null,
                               @BeanProperty
                               var classifiers: java.util.List[PlacesClassifiersDTO] = null,
                               @BeanProperty
                               var phone: String = null,
                               @BeanProperty
                               var href: String = null,
                               @BeanProperty
                               var address: String = null,
                               @BeanProperty
                               var owner: String = null,
                               @BeanProperty
                               var postcode: String = null) {
    def this() = this(null)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesClassifiersDTO(@BeanProperty
                                var category: String = null,
                                @BeanProperty
                                var `type`: String = null,
                                @BeanProperty
                                var subcategory: String = null) {
    def this() = this(null)
}

