package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import com.sonar.expedition.scrawler.util
import cascading.tuple.Fields
import util.Tuples
import com.sonar.dossier.dto.CheckinDTO

class IndividualCheckinJob(args: Args) extends Job(args) with CheckinSource {
    val individual = args("profileIds").split(',').toSet[String]
    SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO).flatMapTo('checkinDto ->('lat, 'lng)) {
        checkin: CheckinDTO =>
            if (individual(checkin.profileId)) Some((checkin.latitude, checkin.longitude))
            else None
    }.write(SequenceFile(args("filteredOut")))
}

