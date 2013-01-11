package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields

class OldSchoolCheckinsExportJob(args: Args) extends Job(args) with CheckinSource {
    /*  override def config =
          super.config ++
                  Map("mapred.map.max.attempts" -> "20",
                      "mapred.reduce.max.attempts" -> "20",
                      "mapred.max.tracker.failures" -> "20")
  */
    val (checkins, _) = checkinSource(args, false, false)
    val checkinsOutput = args("checkinsOut")
    checkins.write(SequenceFile(checkinsOutput, Fields.ALL))

}
