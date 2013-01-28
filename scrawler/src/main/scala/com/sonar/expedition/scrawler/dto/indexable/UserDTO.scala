package com.sonar.expedition.scrawler.dto.indexable

import reflect.BeanProperty
import org.apache.lucene.document._
import com.sonar.dossier.dto.GeodataDTO
import org.apache.lucene.document.Field.Index

case class UserDTO(@BeanProperty name: String,
                   @BeanProperty categories: List[String],
                   @BeanProperty geoData: GeodataDTO,
                   @BeanProperty ip: String) extends Indexable {

    def getDocument() = {
        val doc = new Document
        doc.add(new StringField(IndexField.Name.toString, name, Field.Store.YES))
        doc.add(new TextField(IndexField.Categories.toString, categories.mkString(" "), Field.Store.YES))
        if (geoData != null) doc.add(new LongField(IndexField.Geohash.toString, geoData.geoHash, Field.Store.YES))
        if (ip != null) doc.add(new StringField(IndexField.Ip.toString, ip, Field.Store.YES))
        doc
    }
}