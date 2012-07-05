package com.sonar.expedition.scrawler.dto

import reflect.BeanProperty
import annotation.target.field
import org.codehaus.jackson.map.annotate.JsonDeserialize
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings

case class PlacesGeometryDTO(@BeanProperty
                             var `type`: String,
                             @BeanProperty
                             var coordinates: Array[Double]) {

    //    def this() = this(0, 0)

}
