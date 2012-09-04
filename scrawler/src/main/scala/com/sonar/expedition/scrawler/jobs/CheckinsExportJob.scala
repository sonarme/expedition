package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields

class CheckinsExportJob(args: Args) extends Job(args) with CheckinSource {
    val (checkins, _) = checkinSource(args, false, false)
    val checkinsOutput = args("output")
    checkins.write(SequenceFile(checkinsOutput, Fields.ALL))
            .limit(180000).write(SequenceFile(checkinsOutput + "_small", Fields.ALL))
}
