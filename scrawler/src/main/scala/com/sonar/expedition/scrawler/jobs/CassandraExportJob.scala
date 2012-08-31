package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields

class CassandraExportJob(args: Args) extends Job(args) with CheckinSource {
    val (checkins, _) = checkinSource(args, false, false)
    val checkinsOutput = args("checkinsOut")
    checkins.write(SequenceFile(checkinsOutput, Fields.ALL))

}
