package com.sonar.expedition.scrawler.dto.indexable

import reflect.BeanProperty
import org.apache.lucene.document._
import com.sonar.dossier.dto.GeodataDTO

case class UserDTO(override val key: String,
                @BeanProperty name: String,
                @BeanProperty content: String,
                @BeanProperty categories: List[String],
                @BeanProperty geoData: GeodataDTO,
                @BeanProperty ip: String) extends Indexable(key) {

    def getDocument() = {
        val doc = new Document
        doc.add(new StringField(IndexField.Key.toString, key, Field.Store.YES))
        doc.add(new StringField(IndexField.Name.toString, name, Field.Store.YES))
        doc.add(new TextField(IndexField.Content.toString, content, Field.Store.YES))
        doc.add(new TextField(IndexField.Categories.toString, categories.mkString(" "), Field.Store.YES))
        doc.add(new LongField(IndexField.Geohash.toString, geoData.geoHash, Field.Store.YES))
        doc.add(new StringField(IndexField.Ip.toString, ip, Field.Store.YES))
        doc
    }
}