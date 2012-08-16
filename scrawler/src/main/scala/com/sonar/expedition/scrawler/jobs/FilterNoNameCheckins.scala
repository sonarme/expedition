package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction

class FilterNoNameCheckins(args: Args) extends Job(args) {

    val checkinData = args("checkinData")
    val checkinsWithNamesOutput = args("checkinsWithNamesOutput")
    val checkinGrouperPipe = new CheckinGrouperFunction(args)

    val checkinPipe = checkinGrouperPipe.unfilteredCheckins(TextLine(checkinData).read)
            .filter(('venName)) {
        fields: (String) =>
            val (venName) = fields
            (venName != "")
    }.map(('loc) ->('lat, 'lng)) {
        fields: (String) =>
            val loc = fields
            val lat = loc.split(":").head
            val long = loc.split(":").last
            (lat, long)
    }
            .discard('loc)
            .filter(('lat, 'lng)) {
        fields: (String, String) =>
            val (lat, lng) = fields
            (lat.toDouble > 40.7 && lat.toDouble < 40.9 && lng.toDouble > -74 && lng.toDouble < -73.8)

    }
            .groupBy(('venName, 'ghash)) {
        _.toList[String]('venName -> 'uniqueVenues)
    }
            .project('uniqueVenues)
            //            .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour)
            .write(TextLine(checkinsWithNamesOutput))

}
