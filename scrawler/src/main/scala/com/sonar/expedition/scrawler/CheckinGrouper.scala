// package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouper._
import util.matching.Regex
import grizzled.slf4j.Logging


class CheckinGrouper(args: Args) extends Job(args) {

    var inputData = "/tmp/checkinData.txt"
    var out = "/tmp/userGroupedCheckins.txt"
    //   logger.debug(checkin.getUserProfileId() + "::" + checkin.getServiceType() + "::" + checkin.getServiceProfileId() + "::" + checkin.getServiceCheckinId() + "::" + checkin.getVenueName() + "::" + checkin.getVenueAddress() + "::" + checkin.getCheckinTime() + "::" + checkin.getGeohash() + "::" + checkin.getLatitude() + "::" + checkin.getLongitude() + "::" + checkin.getMessage())
    var data = (TextLine(inputData).read.project('line).map(('line) ->('id, 'serviceType, 'serviceID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geoHash, 'lat, 'lng, 'message)) {
        line: String => {
            line match {
                case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message)
                case _ => ("0","1","2","3","4","5","6","7","8","9","10")
            }
        }
    }).groupBy('id) {
        val data = new Tuple10('serviceType, 'serviceID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geoHash, 'lat, 'lng, 'message)
        group => group.toList[Tuple10[String,String,String,String,String,String,String,String,String,String]](data,'iid)
    }.write(TextLine(out))

}


object CheckinGrouper {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}



