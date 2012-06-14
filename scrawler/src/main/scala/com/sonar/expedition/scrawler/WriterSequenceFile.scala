package com.sonar.expedition.scrawler

import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import cascading.tuple.Fields
import cascading.scheme.hadoop.WritableSequenceFile
import com.twitter.scalding.{Mappable, Source, FixedPathSource}
import org.apache.hadoop.io.Text

case class WriterSeqFile(p: String, f: Fields = Fields.ALL) extends FixedPathSource(p) with WriterSequenceFileScheme

trait WriterSequenceFileScheme extends Mappable[String] {
    //override these as needed:
    val fields = Fields.ALL

    // TODO Cascading doesn't support local mode yet
    override def hdfsScheme = {
        new WritableSequenceFile(new Fields("offset", "line"), classOf[Text], classOf[Text]).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
    }
}