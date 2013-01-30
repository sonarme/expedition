package com.sonar.expedition.scrawler.source

import cascading.scheme.{SourceCall, SinkCall, Scheme}
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import cascading.tuple.{Tuple => CTuple, TupleEntry}
import cascading.flow.FlowProcess
import cascading.tap._

class LuceneScheme(outputFormatClass: Class[_ <: LuceneIndexOutputFormat[_, _]]) extends Scheme[JobConf, RecordReader[CTuple, TupleEntry], OutputCollector[CTuple, TupleEntry], Array[Any], Void] {
    def sourceConfInit(p1: FlowProcess[JobConf], p2: Tap[JobConf, RecordReader[CTuple, TupleEntry], OutputCollector[CTuple, TupleEntry]], p3: JobConf) {
        throw new TapException("LuceneScheme can only be used as a sink, not a source")
    }

    def sinkConfInit(p1: FlowProcess[JobConf], p2: Tap[JobConf, RecordReader[CTuple, TupleEntry], OutputCollector[CTuple, TupleEntry]], conf: JobConf) {
        conf.setOutputKeyClass(classOf[CTuple])
        conf.setOutputValueClass(classOf[TupleEntry])
        conf.setOutputFormat(outputFormatClass)
    }

    def source(p1: FlowProcess[JobConf], p2: SourceCall[Array[Any], RecordReader[CTuple, TupleEntry]]) =
        throw new TapException("LuceneScheme can only be used as a sink, not a source")

    def sink(p1: FlowProcess[JobConf], sinkCall: SinkCall[Void, OutputCollector[CTuple, TupleEntry]]) {
        sinkCall.getOutput.collect(CTuple.NULL, sinkCall.getOutgoingEntry)
    }
}
