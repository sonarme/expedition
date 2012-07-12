package com.sonar.expedition.scrawler.util

import java.util.ArrayList
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.store.Directory
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.util.Version
import LuceneIndex._

object LuceneIndex {
    private[util] var directory: Directory = null
    private[util] var analyzer: Analyzer = null
    private[util] var config: IndexWriterConfig = null
    private[util] var writer: IndexWriter = null
    private var docs: ArrayList[Document] = null
}

class LuceneIndex extends Serializable {

    def initialise() {
        directory = new RAMDirectory()
        docs = new ArrayList[Document]
    }

    def addItems(key: String, value: String) {
        val doc: Document = new Document()
        doc.add(new Field("id", key, Field.Store.YES, Field.Index.ANALYZED))
        doc.add(new Field("content", value, Field.Store.YES, Field.Index.ANALYZED))
        docs.add(doc)
    }

    def search(srchkey: String): String = {
        analyzer = new StandardAnalyzer(Version.LUCENE_35)
        config = new IndexWriterConfig(Version.LUCENE_35, analyzer)
        writer = new IndexWriter(directory, config)
        writer.addDocuments(docs)
        writer.close
        val reader: IndexReader = IndexReader.open(directory)
        val searcher: IndexSearcher = new IndexSearcher(reader)
        val parser: QueryParser = new QueryParser(Version.LUCENE_35, "content", analyzer)
        if (srchkey == null || srchkey.trim == "")
            return "unclassified"
        val query: Query = parser.parse(srchkey)
        val hits: Array[ScoreDoc] = searcher.search(query, 1000).scoreDocs
        //println(srchkey + hits.length)
        hits.collectFirst {
            case hit: ScoreDoc if (hit.doc > 0) =>
                val document = searcher.doc(hit.doc)
                //println(document.get("content") + "--- " + document.get("id"))
                document

        }.headOption.map(_.get("id")).getOrElse("unclassified") //todo: fix this so that the method returns an Option[String]
    }

    def closeWriter() {
        analyzer = new StandardAnalyzer(Version.LUCENE_35)
        config = new IndexWriterConfig(Version.LUCENE_35, analyzer)
        writer = new IndexWriter(directory, config)
        writer.addDocuments(docs)
        writer.close
    }


}
