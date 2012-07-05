package com.sonar.expedition.scrawler.dto

import reflect.BeanProperty
import annotation.target.field
import org.codehaus.jackson.map.annotate.JsonDeserialize
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings

case class PlacesDTO(@BeanProperty
                     var geometry: PlacesGeometryDTO,
                     @BeanProperty
                     var `type`: String,
                     @BeanProperty
                     var id: String,
                     @BeanProperty
                     var properties: PlacesPropertiesDTO) {

    //    def this() = this(0, 0)

}
