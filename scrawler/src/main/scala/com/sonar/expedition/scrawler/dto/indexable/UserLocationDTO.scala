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
        doc.add(new Field(IndexField.ServiceId.toString, serviceId, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        if (geoData != null) doc.add(new Field(IndexField.Geosector.toString, GeoHash.withBitPrecision(geoData.latitude, geoData.longitude, 32).longValue().toString, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        if (ip != null) doc.add(new Field(IndexField.Ip.toString, ip, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        if (timeSegment != null) doc.add(new Field(IndexField.TimeSegment.toString, timeSegment.toIndexableString, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        doc
    }
}