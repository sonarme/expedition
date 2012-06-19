package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.CheckinObjects

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouper._
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}


class CheckinGrouper(args: Args) extends Job(args) {

    var inputData = "/tmp/checkinData.txt"
    var out = "/tmp/userGroupedCheckins.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message)) {
        line: String => {
            line match {
                case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message)
                case _ => ("None","None","None","None","None","None","None","None","None","None","None")
            }
        }
    }).pack[CheckinObjects](('serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message) -> 'checkin).groupBy('userProfileId) {

        group => group.toList[CheckinObjects]('checkin,'checkindata)
    }.map(Fields.ALL -> ('ProfileId, 'venue)){
        fields : (String,List[CheckinObjects]) =>
        val (userid ,checkin) = fields
        val venue = getVenue(checkin)
        (userid,venue)
    }.project('ProfileId,'venue).write(TextLine(out))

    def getVenue(checkins:List[CheckinObjects]) : String ={

        checkins.map(_.getVenueName).toString
    }

}

object CheckinGrouper {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}



