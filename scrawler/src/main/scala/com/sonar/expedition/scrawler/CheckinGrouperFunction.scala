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

class CheckinGrouperFunction(args : Args) extends Job(args) {
//    implicit def p2rp(pipe : Pipe) = new RichPipe(pipe)
//    implicit def rp2p(rp : RichPipe) = rp.pipe
//    implicit def source2rp(src : Source) : RichPipe = RichPipe(src.read)
//
//
//    def name : String = getClass.getCanonicalName
//    implicit val flowDef = {
//        val fd = new FlowDef
//        fd.setName(name)
//        fd
//    }

    def groupCheckins(input : RichPipe): RichPipe = {


        val data = input
                .mapTo('line -> ('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
            line: String => {
                line match {
                    case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
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

//}
}

object CheckinGrouperFunction {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}



