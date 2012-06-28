package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.{CheckinGrouperFunction, CheckinObjects}
import java.security.MessageDigest
import cascading.pipe.{Each, Pipe}
import com.twitter.scalding.TextLine
import cascading.flow.FlowDef

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import com.sonar.expedition.scrawler.CheckinGrouperFunction._
import java.nio.ByteBuffer
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}
import java.util.Calendar

class CheckinGrouperFunction(args : Args) extends Job(args) {



    def groupCheckins(input : RichPipe): RichPipe = {


        val data = input
                .mapTo('line -> ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'message, 'dayOfWeek, 'hour)) {
            line: String => {
                line match {
                    case DataExtractLine(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, mess) => {
                        val timeFilter = Calendar.getInstance()
                        val parsedCheckinTime = checkinTime.replaceFirst("T","").reverse.replaceFirst(":","").reverse
                        val checkinDate = CheckinTimeFilter.parseDateTime(parsedCheckinTime)
                        timeFilter.setTime(checkinDate)
                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        val time = timeFilter.get(Calendar.HOUR_OF_DAY) + timeFilter.get(Calendar.MINUTE)/60.0
                        (id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, mess, dayOfWeek, time)
                    }
                    case _ => ("None","None","None","None","None","None","None","None","None","None","None", 1, 0.0)
                }
            }
        }
                .filter('dayOfWeek){
            dayOfWeek : Int => dayOfWeek > 1 && dayOfWeek < 7
        }.filter('hour){
            hour : Double => hour > 8.5 && hour < 18
        }.map(('latitude, 'longitude) -> ('loc)) {
            fields: (String, String) =>
                val (lat, lng) = fields
                val loc = lat + ":" + lng
                (loc)
        }

//                .pack[CheckinObjects](('serviceType, 'serviceProfileId, 'serviceCheckinId, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude) -> 'checkin)
//                .groupBy('userProfileId) {
//            group => group.toList[CheckinObjects]('checkin,'checkindata)
//        }.map(Fields.ALL -> ('ProfileId, 'venue)){
//            fields : (String,List[CheckinObjects]) =>
//                val (userid ,checkin) = fields
//                val venue = checkin.map(_.getVenueName)
//                (userid,venue)
//        }.project('ProfileId,'venue)

                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

        data
    }

}

object CheckinGrouperFunction {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::([\d]*)::([\.\d\-]*)::([\.\d\-]*)::(.*)""".r
}



