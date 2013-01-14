package com.sonar.expedition.scrawler.service

import org.apache.lucene.store.{RAMDirectory, Directory}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import java.util.Date
import org.apache.lucene.search.{TopDocs, Query, NumericRangeQuery, IndexSearcher}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.queries.mlt.MoreLikeThis
import com.sonar.expedition.scrawler.dto.indexable.IndexField.IndexField
import com.sonar.expedition.scrawler.dto.indexable.IndexField
import com.sonar.expedition.scrawler.dto.indexable.UserDTO

//TODO: Spring
class SearchServiceImpl extends SearchService {
    val directory: Directory = new RAMDirectory
    val analyzer: Analyzer = new StandardAnalyzer(Version.LUCENE_40)
    val iwc: IndexWriterConfig = new IndexWriterConfig(Version.LUCENE_40, analyzer).setOpenMode(OpenMode.CREATE)
    val writer: IndexWriter = new IndexWriter(directory, iwc)

    def index(users: Seq[UserDTO]) {
        val start = new Date()

        users.foreach(_.index(writer))
        writer.close()

        val end = new Date()
        println(end.getTime - start.getTime + " total milliseconds")
    }

    def search(field: IndexField, queryStr: String) = {
        val indexReader = DirectoryReader.open(directory)
        val indexSearcher = new IndexSearcher(indexReader)
        val queryParser = new QueryParser(Version.LUCENE_40, field.toString, analyzer)

        val query = field match {
            case IndexField.Geohash => NumericRangeQuery.newLongRange(IndexField.Geohash.toString, queryStr.toLong, queryStr.toLong, true, true)
            case _ => queryParser.parse(queryStr)
        }
        println("  Searching for: " + query.toString(field.toString))

        val startSearch = new Date()
        val hits = indexSearcher.search(query, 10)
        val endSearch = new Date()
        println("  Search Time: " + (endSearch.getTime - startSearch.getTime) + " ms")

        explain(indexSearcher, query, hits)

        hits
    }

    def moreLikeThis(docNum: Int, moreLikeThisfields: List[IndexField]) = {
        val reader = DirectoryReader.open(directory)

        val mlt = new MoreLikeThis(reader)
        mlt.setBoost(true)
        mlt.setAnalyzer(analyzer)
        mlt.setMinTermFreq(1)
        mlt.setMinDocFreq(1)
        mlt.setFieldNames(moreLikeThisfields.map(_.toString).toArray)

        val q = mlt.like(docNum)
        val is = new IndexSearcher(reader)

        val more = is.search(q, 10)

        explain(is, q, more)
        more
    }

    def moreLikeThis(docNum: Int) = {
        val moreLikeThisFields = List[IndexField](IndexField.Name, IndexField.Content, IndexField.Categories)
        moreLikeThis(docNum, moreLikeThisFields)
    }

    def explain(indexSearcher: IndexSearcher, query: Query, hits: TopDocs) {
        hits.scoreDocs.foreach(hit => {
            val doc = indexSearcher.doc(hit.doc)
            val key = doc.get(IndexField.Key.toString)
            println(key)
            val explanation = indexSearcher.explain(query, hit.doc)
            println(explanation.toString)
        })
    }

    def numDocs = DirectoryReader.open(directory).numDocs()
}