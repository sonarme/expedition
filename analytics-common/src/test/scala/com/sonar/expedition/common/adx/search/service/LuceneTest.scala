package com.sonar.expedition.common.adx.search.service

import org.apache.lucene.index.FieldInfo.IndexOptions
import org.apache.lucene.index.{Term, IndexWriterConfig, IndexWriter}
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.document.Field.Store
import org.scalatest.{BeforeAndAfter, FlatSpec}

import org.apache.lucene.document._

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store._
import org.apache.lucene.search._
import collection.mutable.ListBuffer

class LuceneTest extends FlatSpec with BeforeAndAfter {

    val DocKeyField = "docKey"

    val TermFieldName = "term"
    val TermFieldType = new FieldType
    TermFieldType.setIndexed(true)
    TermFieldType.setOmitNorms(true)
    TermFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS)
    TermFieldType.setTokenized(false)
    TermFieldType.setStoreTermVectors(true)
    TermFieldType.setStored(false)
    TermFieldType.freeze()

    val config = new IndexWriterConfig(Version.LUCENE_41, new StandardAnalyzer(Version.LUCENE_41)).setOpenMode(OpenMode.CREATE_OR_APPEND)
    before {

    }
    "Lucene" should "return the correct DF when storing multiple documents with the same terms into a field" in {


        val directory = new RAMDirectory

        // WRITE
        val writer = new IndexWriter(directory, config)
        try {
            // write 10 documents
            for (i <- 0 to 9) {
                val doc = new Document

                doc.add(new StringField(DocKeyField, "Key_" + i, Store.YES))
                // the documents token field is either Term_0 or Term_1
                doc.add(new Field(TermFieldName, "Term_" + (i % 2), TermFieldType))

                writer.addDocument(doc)
                writer.commit()
            }
        } finally {
            writer.close()
        }


        // READ
        val reader = DirectoryReader.open(directory)
        try {
            val searcher = new IndexSearcher(reader)
            // look up document with key=Key_1
            val docs = searcher.search(new TermQuery(new Term(DocKeyField, "Key_0")), 10).scoreDocs
            val docId = docs.head.doc

            // look up term vector from that document
            val vec = reader.getTermVector(docId, TermFieldName)

            val termsEnum = vec.iterator(null)
            val results = ListBuffer[(String, Long, Long, Int, Int)]()

            while (termsEnum.next() != null) {
                val text = termsEnum.term.utf8ToString()
                val term = new Term(TermFieldName, text)
                // look up TF and DF from reader
                val readerTermFreq = reader.totalTermFreq(term)
                val readerDocFreq = reader.docFreq(term)
                // add term name and stats to result buffer
                results += ((text, termsEnum.totalTermFreq(), readerTermFreq, termsEnum.docFreq(), readerDocFreq))
            }

            assert(results === Seq(("Term_0", 5, 5, 5, 5)))
        } finally {
            reader.close()
        }
    }

}
