package com.sonar.expedition.scrawler.source

import com.twitter.scalding._
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

case class LuceneSource(path: String,
                        outputFormatClass: Class[_ <: LuceneIndexOutputFormat[_, _]]) extends FixedPathSource(path) {

    override def hdfsScheme = new LuceneScheme(outputFormatClass).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
}
