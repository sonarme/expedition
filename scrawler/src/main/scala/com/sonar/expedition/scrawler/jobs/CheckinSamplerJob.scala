package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import cascading.tuple.Fields

class CheckinSamplerJob(args: Args) extends DefaultJob(args) with CheckinSource with DTOProfileInfoPipe {
    val venues = args("venues").split(',').toSet[String]
    val (checkins, checkinsWithVenue) = checkinSource(args, false, true)
    val profiles = serviceProfiles(args)
    checkins.filter('venId) {
        goldenId: String => venues(goldenId)
    }.write(Tsv("s3n://scrawler/sampleCheckins/raw", Fields.ALL, true, true))
    checkinsWithVenue.filter('goldenId) {
        goldenId: String => venues(goldenId)
    }.write(Tsv("s3n://scrawler/sampleCheckins/withVenue", Fields.ALL, true, true))
            .joinWithLarger('keyid -> 'key, profiles)
            .write(Tsv("s3n://scrawler/sampleCheckins/final", Fields.ALL, true, true))
}
