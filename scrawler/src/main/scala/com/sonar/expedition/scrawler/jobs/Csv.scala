package com.sonar.expedition.scrawler.jobs


import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}
import com.twitter.scalding._
import cascading.scheme.Scheme
import org.apache.hadoop.mapred._
import cascading.tuple.Fields

case class Csv(p: String, f: Fields = Fields.ALL) extends FixedPathSource(p)
with DelimitedScheme {
    override val fields = f
    override val separator = ","
    override val writeHeader = true

    override def hdfsScheme = {
        val scheme = new CHTextDelimited(fields, skipHeader, writeHeader, separator, types).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
        scheme.setNumSinkParts(1)
        scheme
    }
}
