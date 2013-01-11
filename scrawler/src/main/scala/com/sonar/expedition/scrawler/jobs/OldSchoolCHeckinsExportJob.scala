package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields

class OldSchoolCheckinsExportJob(args: Args) extends Job(args) with CheckinSource {

    override def config =
           super.config ++
                   Map("mapred.map.tasks" -> "50")

    val (checkins, _) = checkinSource(args, false, false)
    val checkinsOutput = args("checkinsOut")
    checkins.write(SequenceFile(checkinsOutput, Fields.ALL))

}
