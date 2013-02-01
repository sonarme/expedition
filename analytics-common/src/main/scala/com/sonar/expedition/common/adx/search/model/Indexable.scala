package com.sonar.expedition.common.adx.search.model

import java.util.UUID
import org.apache.lucene.document.{StringField, Field, Document}
import org.apache.lucene.index.IndexWriter

abstract class Indexable {

    val key: String = UUID.randomUUID.toString.replaceAll("-", "")

    def getDocument(): Document

    def index(writer: IndexWriter) = {
        val doc = getDocument()
        doc.add(new StringField(IndexField.Key.toString, key, Field.Store.YES))
        writer.addDocument(doc)
        doc
    }
}
