package com.sonar.expedition.common.adx.search.model

import reflect.BeanProperty
import com.sonar.dossier.dto.GeodataDTO
import org.apache.lucene.document.{LongField, Field, StringField, Document}
import ch.hsr.geohash.GeoHash

case class UserLocation(serviceId: String,
                        lat: Double,
                        lng: Double,
                        ip: String,
                        timeSegment: String) extends Indexable {
    lazy val geosector = GeoHash.withCharacterPrecision(lat, lng, 7).toBase32

    def getDocument() = {

        val doc = new Document
        doc.add(new StringField(IndexField.ServiceId.toString, serviceId, Field.Store.YES))
        doc.add(new StringField(IndexField.Geosector.toString, geosector, Field.Store.YES))
        if (ip != null) doc.add(new StringField(IndexField.Ip.toString, ip, Field.Store.YES))
        doc
    }
}
