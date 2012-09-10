package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import cascading.tuple.Fields

class CheckinSamplerJob(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe {
    val venues = args("venues").split(',').toSet[String]
    val (_, checkins) = checkinSource(args, false, true)
    val profiles = serviceProfiles(args)
    checkins.filter('goldenId) {
        goldenId: String => venues(goldenId)
    }.joinWithLarger('keyid -> 'key, profiles).write(Tsv(args("output"), Fields.ALL, true, true))
}
