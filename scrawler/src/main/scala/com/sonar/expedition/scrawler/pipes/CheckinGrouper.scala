package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import java.nio.ByteBuffer
import util.matching.Regex
import grizzled.slf4j.Logging

class CheckinGrouper(args: Args) extends Job(args) {


    // reads in checkin data then packs them into checkinobjects, grouped by sonar id. objects will need to be joined with service profile data and friend data
    var inputData = "/tmp/checkinData.txt"
    var out = "/tmp/userGroupedCheckins.txt"
    var data = (TextLine(inputData).read.project('line).flatMap(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinId, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
        line: String => {
            line match {
                case CheckinExtractLine(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng) => Some((id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng))
                case _ => None
            }
        }
    }).project(('userProfileId, 'venueName, 'latitude, 'longitude)).write(TextLine(out))


}

object CheckinGrouper {

}



