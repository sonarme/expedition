package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.CheckinObjects
import java.security.MessageDigest

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouper._
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}

class CheckinGrouper(args: Args) extends Job(args) {


    // extracts key data for employerfinder to use. no longer groups
    val inputData = "/tmp/tcheckinData.txt"
    val out = "/tmp/userGroupedCheckins.txt"
    val data = (TextLine(inputData).read.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
        line: String => {
            line match {
                case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                case _ => ("None","None","None","None","None","None","None","None","None","None")
            }
        }
    })
//            .pack[CheckinObjects](('serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude) -> 'checkin)
//            .groupBy('userProfileID) {
//        group => group
//                .toList[String]('venueName,'venue)
//            .toList[String]('latitude, 'lat)
//            .toList[String]('longitude, 'lng)
//    }
//            .map(Fields.ALL -> ('ProfileID, 'venue, 'lat, 'lng)){
//        fields : (String,List[CheckinObjects]) =>
//            val (userid ,checkin) = fields
//            val venue = checkin.map(_.getVenueName)
//            val lat = checkin.map(_.getLatitude)
//            val long = checkin.map(_.getLongitude)
//            (userid,venue,lat,long)
//    }
            .project(('userProfileID,'venueName,'latitude,'longitude)).write(TextLine(out))



//     .project(Fields.ALL).discard(0).map(Fields.ALL -> ('ProfileID, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng)){
//        fields : (String, String, String, String, String, String, String, String, String, String) =>
//            val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)    = fields
//            val hashedServiceID = md5SumString(serviceID.getBytes("UTF-8"))
//            (id, serviceType, hashedServiceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
//    }.project('ProfileID, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng).write(TextLine(out))
//
//
//    def md5SumString(bytes : Array[Byte]) : String = {
//        val md5 = MessageDigest.getInstance("MD5")
//        md5.reset()
//        md5.update(bytes)
//        md5.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
//    }

}

object CheckinGrouper {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}



