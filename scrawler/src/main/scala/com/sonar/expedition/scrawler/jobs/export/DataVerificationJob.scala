package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.{Tsv, Args, SequenceFile}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.jobs.{CheckinSource, DefaultJob}
import com.sonar.dossier.dto.ServiceProfileLink

class DataVerificationJob(args: Args) extends DefaultJob(args) with CheckinSource {
    val newIn = args("newIn")
    val oldIn = args("oldIn")

    val dataType = args("dataType")


    val oldPipe =
        dataType match {
            case "checkin" =>
                SequenceFile(oldIn, ('serType, 'serCheckinID)).read.mapTo(('serType, 'serCheckinID) -> 'id) {
                    in: (String, String) => in._1 + ":" + in._2
                }
            case "profile" =>
                SequenceFile(oldIn, 'profileId).read.rename('profileId -> 'id)
        }
    val newPipe = dataType match {
        case "checkin" =>
            SequenceFile(newIn, 'id).read
        case "profile" =>
            SequenceFile(newIn, 'profileId).read.mapTo('profileId -> 'id) {
                profileId: ServiceProfileLink => profileId.profileId
            }
    }
    val oldStat = oldPipe.groupAll {
        _.size
    }.map(() -> 'statName) {
        _: Unit => "old"
    }.project('statName, 'size)
    val newStat = oldPipe.joinWithLarger('id -> 'id, newPipe).groupAll {
        _.size
    }.map(() -> 'statName) {
        _: Unit => "new"
    }.project('statName, 'size)
    (oldStat ++ newStat).write(Tsv(newIn + "_compare_stats", ('statName, 'size)))

}
