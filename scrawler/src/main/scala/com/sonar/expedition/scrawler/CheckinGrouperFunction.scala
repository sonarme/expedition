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
                .mapTo('line -> ('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
            line: String => {
                line match {
                    case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) => {
                        val timeFilter = Calendar.getInstance()
                        val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                        timeFilter.setTime(checkinDate)
                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        //todo: filter that day of week is > 1 < 7
                        (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                    }
                    case _ => ("None","None","None","None","None","None","None","None","None","None")
                }
            }
        }
//                .pack[CheckinObjects](('serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude) -> 'checkin)
//                .groupBy('userProfileID) {
//            group => group.toList[CheckinObjects]('checkin,'checkindata)
//        }.map(Fields.ALL -> ('ProfileID, 'venue)){
//            fields : (String,List[CheckinObjects]) =>
//                val (userid ,checkin) = fields
//                val venue = checkin.map(_.getVenueName)
//                (userid,venue)
//        }.project('ProfileID,'venue)

                .project(('userProfileID,'venueName,'latitude,'longitude))

        data
    }

}

object CheckinGrouperFunction {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}



