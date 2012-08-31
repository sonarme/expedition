package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, SequenceFile, Args, Job}
import cascading.tuple.Fields

class LimitCheckinsJob(args: Args) extends Job(args) with CheckinSource {
    val in = args("in")
    val out = args("out")
    SequenceFile(in, CheckinTuple).read.limit(180000).write(SequenceFile(out, Fields.ALL))

}
