package com.sonar.expedition.scrawler.service

import org.apache.lucene.store.{FSDirectory, RAMDirectory, Directory}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index._
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import java.util.Date
import org.apache.lucene.search._
import com.sonar.expedition.scrawler.dto.indexable.IndexField.IndexField
import com.sonar.expedition.scrawler.dto.indexable.{Indexable, IndexField, UserDTO}
import reflect.BeanProperty
import org.apache.lucene.queries.mlt._
import org.apache.lucene.queries._
import org.apache.lucene.queryparser.classic.QueryParser

//TODO: Spring
class SearchServiceImpl(@BeanProperty directory: Directory, @BeanProperty create: Boolean = false) extends SearchService {
    val analyzer: Analyzer = new StandardAnalyzer(Version.LUCENE_41)

    def index(indexable: Indexable) = {
        val start = new Date()

        val iwc: IndexWriterConfig = new IndexWriterConfig(Version.LUCENE_41, analyzer).setOpenMode(if (create) OpenMode.CREATE else OpenMode.CREATE_OR_APPEND)
        val writer: IndexWriter = new IndexWriter(directory, iwc)
        val doc = indexable.index(writer) //todo: implement some kind of locking
        writer.close()

        val end = new Date()
        println(end.getTime - start.getTime + " total milliseconds")
        doc
    }

    def index(indexables: Seq[Indexable]) {
        val start = new Date()

        val iwc: IndexWriterConfig = new IndexWriterConfig(Version.LUCENE_41, analyzer).setOpenMode(if (create) OpenMode.CREATE else OpenMode.CREATE_OR_APPEND)
        val writer: IndexWriter = new IndexWriter(directory, iwc)
        indexables.foreach(_.index(writer))
        writer.close()

        val end = new Date()
        println(end.getTime - start.getTime + " total milliseconds")
    }

    def search(field: IndexField, queryStr: String) = {
        val indexReader = DirectoryReader.open(directory)
        val indexSearcher = new IndexSearcher(indexReader)
        val queryParser = new QueryParser(Version.LUCENE_41, field.toString, analyzer)

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
        val start = new Date()
        val mlt = new MoreLikeThis(reader)
        //        mlt.setBoost(true)
        mlt.setAnalyzer(analyzer)
        mlt.setMinTermFreq(1)
        mlt.setMinDocFreq(1)
        //todo: more like this doesn't seem to work on longfields
        mlt.setFieldNames(moreLikeThisfields.map(_.toString).toArray)

        val q = mlt.like(docNum)
        val is = new IndexSearcher(reader)
        val doc = is.doc(docNum)
        val docKey = doc.get(IndexField.Key.toString)
        val docFilter = new BooleanFilter
        val tf = new TermsFilter(new Term(IndexField.Key.toString, docKey))
        // TOOD: FIX tf.addTerm(new Term(IndexField.Key.toString, docKey))
        docFilter.add(new FilterClause(tf, BooleanClause.Occur.MUST_NOT))

        val more = is.search(q, docFilter, 10)

        val end = new Date()
        println(end.getTime - start.getTime + " total milliseconds (more like this)")
        explain(is, q, more)
        more
    }

    def moreLikeThis(docNum: Int) = {
        val moreLikeThisFields = List[IndexField](IndexField.Name, IndexField.Categories, IndexField.Ip, IndexField.Geosector, IndexField.TimeSegment)
        moreLikeThis(docNum, moreLikeThisFields)
    }

    def explain(indexSearcher: IndexSearcher, query: Query, hits: TopDocs) {
        /*
        hits.scoreDocs.foreach(hit => {
            val doc = indexSearcher.doc(hit.doc)
            val key = doc.get(IndexField.Key.toString)
            println(key)
            val explanation = indexSearcher.explain(query, hit.doc)
            println(explanation.toString)
        })
        */
    }

    def numDocs = DirectoryReader.open(directory).numDocs()
}
