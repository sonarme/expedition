package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.{Tsv, Args, SequenceFile}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.jobs.{CheckinSource, DefaultJob}

class DataVerificationJob(args: Args) extends DefaultJob(args) with CheckinSource {
    val checkinsNew = args("checkinsNewIn")
    val checkinsOld = args("checkinsOldIn")
    val checkinsOldPipe = SequenceFile(checkinsOld, Tuples.Checkin).read.mapTo(('serType, 'serCheckinID) -> 'oldCheckinId) {
        in: (String, String) => in._1 + ":" + in._2
    }
    val checkinsNewPipe = SequenceFile(checkinsNew, Tuples.CheckinIdDTO).read.project('checkinId)

    val oldStat = checkinsOldPipe.groupAll {
        _.size
    }.map(() -> 'statName) {
        _: Unit => "old"
    }.project('statName, 'size)
    val newStat = checkinsOldPipe.joinWithLarger('oldCheckinId -> 'checkinId, checkinsNewPipe).groupAll {
        _.size
    }.map(() -> 'statName) {
        _: Unit => "new"
    }.project('statName, 'size)
    (oldStat ++ newStat).write(Tsv(checkinsNew + "_compare_stats", ('statName, 'size)))

}
