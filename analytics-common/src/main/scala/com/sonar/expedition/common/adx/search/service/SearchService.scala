package com.sonar.expedition.common.adx.search.service

import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.{PriorityQueue, Version}
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.io.StringReader
import com.yammer.metrics.scala.Instrumented
import com.sonar.expedition.common.adx.search.model._
import grizzled.slf4j.Logging

class SearchService(var reader: IndexReader, val writer: IndexWriter = null) extends Instrumented with Logging {
    implicit def moreLikeThis_to_richMoreLikeThis(mlt: MoreLikeThis) = new RichMoreLikeThis(mlt)

    var indexSearcher = new IndexSearcher(reader)
    val mltTimer = metrics.timer("moreLikeThis")
    val searchTimer = metrics.timer("search")

    // TODO: hack
    def reopen() {
        reader match {
            case dr: DirectoryReader =>
                reader = DirectoryReader.openIfChanged(dr)
                indexSearcher = new IndexSearcher(reader)
        }
    }

    def index(indexable: Indexable) = indexMulti(Seq(indexable)).head

    def indexMulti(indexables: Seq[Indexable]) = {
        val docs = indexables.map(_.index(writer))
        writer.commit()
        docs
    }

    def search(field: IndexField.Value, queryStr: String) = searchTimer.time {
        val queryParser = new QueryParser(Version.LUCENE_41, field.toString, new StandardAnalyzer(Version.LUCENE_41))

        val query = field match {
            //            case IndexField.Geosector => NumericRangeQuery.newLongRange(IndexField.Geohash.toString, queryStr.toLong, queryStr.toLong, true, true)
            case _ => queryParser.parse(queryStr)
        }
        info("Searching for: " + query.toString(field.toString))

        val hits = indexSearcher.search(query, 10)

        explain(query, hits)

        hits
    }

    def moreLikeThis(terms: Map[String, String]): TopDocs = mltTimer.time {
        val mlt = new MoreLikeThis(reader)
        //        mlt.setBoost(true)
        mlt.setMinTermFreq(1)
        mlt.setMinDocFreq(1)
        //todo: more like this doesn't seem to work on longfields
        mlt.setAnalyzer(new StandardAnalyzer(Version.LUCENE_41))
        val query = mlt.likeTerms(terms)
        val more = indexSearcher.search(query, 10)

        explain(query, more)
        more
    }

    def doc(docId: Int) = indexSearcher.doc(docId)

    def explain(query: Query, hits: TopDocs) {
        hits.scoreDocs.foreach(hit => {
            val doc = indexSearcher.doc(hit.doc)
            val key = doc.get(IndexField.Key.toString)
            val explanation = indexSearcher.explain(query, hit.doc)
            info("Key: " + key + "  explanation: " + explanation)
        })
    }

    def numDocs = reader.numDocs()

    class RichMoreLikeThis(mlt: MoreLikeThis) {
        def likeTerms(termMap: Map[String, String]) = {
            val fieldNames = termMap.keys.toArray
            mlt.setFieldNames(fieldNames)
            val freq = new FreqQ(termMap.size)
            for ((fieldName, term) <- termMap) {
                val queue = mlt.retrieveTerms(new StringReader(term), fieldName)
                while (queue.size() > 0) {
                    freq.add(queue.pop())
                }
            }
            createQuery(freq)
        }

        /**
         * Create the More like query from a PriorityQueue
         */
        private def createQuery(q: PriorityQueue[Array[AnyRef]]) = {
            val query = new BooleanQuery
            var qterms = 0
            var bestScore = 0f
            while (q.size() > 0 && (mlt.getMaxQueryTerms == 0 || qterms < mlt.getMaxQueryTerms)) {
                val ar = q.pop()
                val tq = new TermQuery(new Term(ar(1).asInstanceOf[String], ar(0).asInstanceOf[String]))
                if (mlt.isBoost) {
                    if (qterms == 0) {
                        bestScore = (ar(2).asInstanceOf[Float])
                    }
                    val myScore = ar(2).asInstanceOf[Float]
                    tq.setBoost(mlt.getBoostFactor * myScore / bestScore)
                }
                query.add(tq, BooleanClause.Occur.SHOULD)
                qterms += 1
            }
            query
        }

        class FreqQ(s: Int) extends PriorityQueue[Array[Object]](s) {
            def lessThan(a: Array[Object], b: Array[Object]) = a(2).asInstanceOf[Float] > b(2).asInstanceOf[Float]
        }

    }

}

