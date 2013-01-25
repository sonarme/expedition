package com.sonar.expedition.scrawler.source

import com.twitter.scalding._
import com.scaleunlimited.cascading.lucene.{LuceneOutputFormat, LuceneScheme}
import cascading.tuple.Fields
import org.apache.lucene.analysis.Analyzer
import com.scaleunlimited.cascading.lucene.LuceneScheme.DefaultAnalyzer
import org.apache.lucene.document.{Field => LField}
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

case class LuceneSource(path: String,
                        schemeFields: Fields,
                        storeSettings: Array[LField.Store] = null,
                        indexSettings: Array[LField.Index] = null,
                        hasBoost: Boolean = false,
                        analyzer: Class[_ <: Analyzer] = classOf[DefaultAnalyzer],
                        maxFieldLength: Int = Int.MaxValue,
                        maxSegments: Int = LuceneOutputFormat.DEFAULT_MAX_SEGMENTS) extends FixedPathSource(path) {

    override def hdfsScheme = new LuceneScheme(schemeFields,
        storeSettings,
        indexSettings,
        hasBoost, analyzer,
        maxFieldLength, maxSegments).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
}
