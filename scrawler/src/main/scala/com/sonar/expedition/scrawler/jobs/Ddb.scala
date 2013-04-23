package com.sonar.expedition.scrawler.jobs

import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}
import com.twitter.scalding._
import cascading.scheme.Scheme
import org.apache.hadoop.mapred._
import cascading.tuple.Fields

case class Ddb(p: String, f: Fields = Fields.ALL,
               override val skipHeader: Boolean = true, override val writeHeader: Boolean = false) extends FixedPathSource(p)
with DelimitedScheme {
    override val fields = f
    override val separator = "\u0002"

    override def hdfsScheme = {
        val scheme = new CHTextDelimited(fields, skipHeader, writeHeader, separator, types).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
        scheme.setNumSinkParts(1)
        scheme
    }
}
