package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.{Tsv, SequenceFile, Args}
import com.sonar.expedition.scrawler.jobs.{CheckinSource, DefaultJob}
import com.sonar.expedition.scrawler.util.Tuples

class CheckinIdJob(args: Args) extends DefaultJob(args) with CheckinSource {
    val newIn = args("checkinsIn")
    SequenceFile(newIn, Tuples.CheckinIdDTO).read.project('checkinId).filter('checkinId) {
        checkinId: String =>
            checkinId != null && checkinId.startsWith("facebook:") && checkinId.contains("_")
    }.write(Tsv(newIn + "_fb_corrupt_ids"))
}
