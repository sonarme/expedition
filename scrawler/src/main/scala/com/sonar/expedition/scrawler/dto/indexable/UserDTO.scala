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
        doc.add(new Field(IndexField.Name.toString, name, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        doc.add(new Field(IndexField.Categories.toString, categories.mkString(" "), Field.Store.YES, Index.ANALYZED_NO_NORMS))
        if (geoData != null) doc.add(new Field(IndexField.Geohash.toString, geoData.geoHash.toString, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        if (ip != null) doc.add(new Field(IndexField.Ip.toString, ip, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        doc
    }
}