package com.sonar.expedition.scrawler.dto.indexable

import reflect.BeanProperty
import com.sonar.dossier.dto.GeodataDTO
import org.apache.lucene.document._
import com.sonar.dossier.dto.GeodataDTO
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.jobs.behavior.TimeSegment
import org.apache.lucene.document.Field.Index

case class UserLocationDTO(@BeanProperty serviceId: String,
                           @BeanProperty geoData: GeodataDTO,
                           @BeanProperty ip: String,
                           @BeanProperty timeSegment: TimeSegment) extends Indexable {

    def getDocument() = {

        val doc = new Document
        doc.add(new StringField(IndexField.ServiceId.toString, serviceId, Field.Store.YES))
        if (geoData != null) doc.add(new LongField(IndexField.Geosector.toString, geoData.getGeoHash, Field.Store.YES))
        if (ip != null) doc.add(new StringField(IndexField.Ip.toString, ip, Field.Store.YES))
        if (timeSegment != null) doc.add(new StringField(IndexField.TimeSegment.toString, timeSegment.toIndexableString, Field.Store.YES))
        doc
    }
}