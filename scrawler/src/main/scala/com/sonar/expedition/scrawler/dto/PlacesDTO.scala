package com.sonar.expedition.scrawler.dto

import reflect.BeanProperty
import org.codehaus.jackson.annotate.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesDTO(@BeanProperty
                     @JsonProperty("geometry")
                     var geometry: PlacesGeometryDTO = PlacesGeometryDTO(),
                     @BeanProperty
                     var `type`: String = null,
                     @BeanProperty
                     @JsonProperty("id")
                     var id: String = null,
                     @BeanProperty
                     @JsonProperty("properties")
                     var properties: PlacesPropertiesDTO = PlacesPropertiesDTO()
                     ) {
    def this() = this(null, null)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesGeometryDTO(@BeanProperty
                             var `type`: String = null,
                             @BeanProperty
                             @JsonProperty("coordinates")
                             var coordinates: Array[Double] = null) {

    def this() = this(null)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesPropertiesDTO(@BeanProperty
                               @JsonProperty("province")
                               var province: String = null,
                               @BeanProperty
                               @JsonProperty("city")
                               var city: String = null,
                               @BeanProperty
                               @JsonProperty("name")
                               var name: String = null,
                               @BeanProperty
                               @JsonProperty("tags")
                               var tags: java.util.List[String] = null,
                               @BeanProperty
                               @JsonProperty("country")
                               var country: String = null,
                               @BeanProperty
                               @JsonProperty("classifiers")
                               var classifiers: java.util.List[PlacesClassifiersDTO] = null,
                               @BeanProperty
                               @JsonProperty("phone")
                               var phone: String = null,
                               @BeanProperty
                               @JsonProperty("href")
                               var href: String = null,
                               @BeanProperty
                               @JsonProperty("address")
                               var address: String = null,
                               @BeanProperty
                               @JsonProperty("owner")
                               var owner: String = null,
                               @BeanProperty
                               @JsonProperty("postcode")
                               var postcode: String = null) {
    def this() = this(null)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class PlacesClassifiersDTO(@BeanProperty
                                @JsonProperty("category")
                                var category: String = null,
                                @BeanProperty
                                var `type`: String = null,
                                @BeanProperty
                                @JsonProperty("subcategory")
                                var subcategory: String = null) {
    def this() = this(null)
}

