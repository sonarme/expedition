package com.sonar.expedition.common.adx.search.service

import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.{BytesRef, PriorityQueue, Version}
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.io.StringReader
import com.yammer.metrics.scala.Instrumented
import com.sonar.expedition.common.adx.search.model._
import grizzled.slf4j.Logging
import collection.mutable.ListBuffer

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

    def searchDoc(field: IndexField.Value, queryStr: String) = {
        val topDocs = search(field, queryStr, 1)
        if (topDocs == null) None
        else
            topDocs.scoreDocs.headOption flatMap {
                scoreDoc => Option(reader.document(scoreDoc.doc))
            }


    }

    def terms(docId: Int, field: IndexField.Value) = {
        val vec = reader.getTermVector(docId, field.toString)
        val termsEnum = vec.iterator(null)
        val results = ListBuffer[BytesRef]()

        while (termsEnum.next() != null) {
            results += termsEnum.term()
        }
        results
    }

    def docFreq(term: Term) = reader.docFreq(term)

    def search(field: IndexField.Value, queryStr: String, n: Int = 10) = searchTimer.time {
        val query = new TermQuery(new Term(field.toString, queryStr))
        val hits = indexSearcher.search(query, n)
        explain(query, hits)
        hits
    }

    def moreLikeThis(terms: Map[String, String], n: Int = 10): TopDocs = mltTimer.time {
        val mlt = new MoreLikeThis(reader)
        //        mlt.setBoost(true)
        mlt.setMinTermFreq(1)
        mlt.setMinDocFreq(1)
        //todo: more like this doesn't seem to work on longfields
        mlt.setAnalyzer(new StandardAnalyzer(Version.LUCENE_41))
        val query = mlt.likeTerms(terms)
        val more = indexSearcher.search(query, n)

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

