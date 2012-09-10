package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import cascading.tuple.Fields
import ch.hsr.geohash.GeoHash

class CheckinGeoSamplerJob(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe {
    val (checkins, checkinsWithVenue) = checkinSource(args, false, true)
    val profiles = serviceProfiles(args)
    checkins.filter('lat, 'lng) {
        in: (Double, Double) => GeoHash.withCharacterPrecision(in._1, in._2, 8).longValue().toString.endsWith("99")
    }.write(Tsv("s3n://scrawler/sampleCheckinsGeo/raw", Fields.ALL, true, true))
    checkinsWithVenue.filter('lat, 'lng) {
        in: (Double, Double) => GeoHash.withCharacterPrecision(in._1, in._2, 8).longValue().toString.endsWith("99")
    }.write(Tsv("s3n://scrawler/sampleCheckinsGeo/withVenue", Fields.ALL, true, true))
            .joinWithLarger('keyid -> 'key, profiles)
            .write(Tsv("s3n://scrawler/sampleCheckinsGeo/final", Fields.ALL, true, true))
}
