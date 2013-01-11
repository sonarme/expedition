package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, SequenceFile, Args, Job}
import com.sonar.expedition.scrawler.util
import cascading.tuple.Fields
import util.Tuples
import com.sonar.dossier.dto.CheckinDTO

class IndividualCheckinJob(args: Args) extends Job(args) with CheckinSource {
    val individual = args("profileIds").split(',').toSet[String]

    val old = args.optional("old").map(_.toBoolean).getOrElse(false)
    if (old)
        SequenceFile(args("checkinsIn"), Tuples.Checkin).read.project('lat, 'lng, 'chknTime).write(Tsv(args("filteredOut")))
    else
        SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO).flatMapTo('checkinDto ->('lat, 'lng, 'checkinTime)) {
            checkin: CheckinDTO =>
                if (individual(checkin.profileId)) Some((checkin.latitude, checkin.longitude, checkin.checkinTime.toDate))
                else None
        }.write(Tsv(args("filteredOut")))
}

