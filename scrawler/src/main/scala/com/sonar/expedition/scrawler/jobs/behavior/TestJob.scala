package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.expedition.scrawler.source.LuceneSource
import elephantdb.cascading.ElephantDBTap.{Args => EArgs}
import scala.Array
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.Field.Index

class TestJob(args: Args) extends DefaultJob(args) {
    IterableSource(Seq(
        ("a", "b1"),
        ("a", "b2"),
        ("a", "b3"),
        ("x", "y1"),
        ("x", "y2")
    ), ('a, 'b)).read
            .write(LuceneSource("TestLucene", ('a, 'b), Array(Store.YES, Store.YES), Array(Index.ANALYZED_NO_NORMS, Index.ANALYZED_NO_NORMS)))
}
