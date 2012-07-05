package com.sonar.expedition.scrawler.dto

import reflect.BeanProperty
import annotation.target.field
import org.codehaus.jackson.map.annotate.JsonDeserialize
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings

case class PlacesPropertiesDTO(@BeanProperty
                               var province: String,
                               @BeanProperty
                               var city: String,
                               @BeanProperty
                               var name: String,
                               @BeanProperty
                               var country: String,
                               @BeanProperty
                               var classifiers: PlacesClassifiersDTO,
                               @BeanProperty
                               var phone: String,
                               @BeanProperty
                               var href: String,
                               @BeanProperty
                               var address: String,
                               @BeanProperty
                               var owner: String,
                               @BeanProperty
                               var postcode: String) {

    //    def this() = this(0, 0)

}
