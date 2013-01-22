package com.sonar.expedition.scrawler.dto.indexable

import reflect.BeanProperty
import org.apache.lucene.document.{Field, Document}
import org.apache.lucene.index.{Term, IndexWriter}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import java.util.UUID
import org.apache.lucene.document.Field.Index

abstract class Indexable {

    val key: String = UUID.randomUUID.toString.replaceAll("-", "")
    def getDocument(): Document

    def index(writer: IndexWriter) = {
        val doc = getDocument()
        doc.add(new Field(IndexField.Key.toString, key, Field.Store.YES, Index.ANALYZED_NO_NORMS))
        writer.addDocument(doc)
        doc
    }
}