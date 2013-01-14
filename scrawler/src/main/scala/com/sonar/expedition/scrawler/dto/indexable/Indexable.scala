package com.sonar.expedition.scrawler.dto.indexable

import reflect.BeanProperty
import org.apache.lucene.document.Document
import org.apache.lucene.index.{Term, IndexWriter}
import org.apache.lucene.index.IndexWriterConfig.OpenMode

abstract class Indexable(@BeanProperty val key: String) {

    def getDocument(): Document

    def index(writer: IndexWriter) {
        val doc = getDocument()
        if (writer.getConfig.getOpenMode == OpenMode.CREATE) {
            println("    adding " + key)
            writer.addDocument(doc)
        } else {
            println("    updating " + key)
            writer.updateDocument(new Term("key", key), doc)
        }
    }
}