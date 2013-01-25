package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{HadoopSchemeInstance, Args}
import com.twitter.scalding.SequenceFile
import cascading.scheme.local.{TextLine => CLTextLine, TextDelimited => CLTextDelimited}
import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile, WritableSequenceFile => CHWritableSequenceFile}
import cascading.tuple.Fields

class SequenceFileLimitJob(args: Args) extends DefaultJob(args) {
    val input = args("input")
    val output = args("output")
    val limit = args("limit").toInt
    SequenceFile(input).limit(limit).write(new LimitedSequenceFile(output))
}

class LimitedSequenceFile(p: String, f: Fields = Fields.ALL, parts: Int = 1) extends SequenceFile(p, f) {
    override def hdfsScheme = {
        val scheme = HadoopSchemeInstance(new CHSequenceFile(fields))
        scheme.setNumSinkParts(parts)
        scheme
    }
}
