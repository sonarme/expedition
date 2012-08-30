package com.sonar.expedition.scrawler.test

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction
import com.twitter.scalding.TextLine

// JUST FOR TESTING
class FilterNoNameCheckins(args: Args) extends Job(args) with CheckinGrouperFunction {

    val checkinData = args("checkinData")
    val checkinsWithNamesOutput = args("checkinsWithNamesOutput")

    val checkinPipe = unfilteredCheckins(TextLine(checkinData).read)
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
