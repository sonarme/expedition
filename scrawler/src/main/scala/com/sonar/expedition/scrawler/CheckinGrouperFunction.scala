//package com.sonar.expedition.scrawler
//
//import cascading.tuple.Fields
//import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
//import com.sonar.dossier.dto.ServiceProfileDTO
//import com.sonar.expedition.scrawler.CheckinObjects
//import java.security.MessageDigest
//import cascading.pipe.{Each, Pipe}
//import com.twitter.scalding.TextLine
//import cascading.flow.FlowDef
//
////import com.sonar.expedition.scrawler.MeetupCrawler
//import com.twitter.scalding._
//import java.nio.ByteBuffer
//import CheckinGrouper._
//import util.matching.Regex
//import grizzled.slf4j.Logging
//import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
//import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}
//
//class CheckinGrouperFunction{
//
//    def groupCheckins(checkinInput : TextLine): Pipe = {
//        // reads in checkin data then packs them into checkinobjects, grouped by sonar id. objects will need to be joined with service profile data and friend data
//        def name : String = getClass.getCanonicalName
//
//        //This is the FlowDef used by all Sources this job creates
//        @transient
//        implicit val flowDef = {
//            val fd = new FlowDef
//            fd.setName(name)
//            fd
//        }
//
//
//        val data = (checkinInput.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
//            line: String => {
//                line match {
//                    case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
//                    case _ => ("None","None","None","None","None","None","None","None","None","None")
//                }
//            }
//        }).pack[CheckinObjects](('serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude) -> 'checkin).groupBy('userProfileID) {
//            group => group.toList[CheckinObjects]('checkin,'checkindata)
//        }.map(Fields.ALL -> ('ProfileID, 'venue)){
//            fields : (String,List[CheckinObjects]) =>
//                val (userid ,checkin) = fields
//                val venue = checkin.map(_.getVenueName)
//                (userid,venue)
//        }.project('ProfileID,'venue)
//        data
//    }
//
//
//    //     .project(Fields.ALL).discard(0).map(Fields.ALL -> ('ProfileID, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng)){
//    //        fields : (String, String, String, String, String, String, String, String, String, String) =>
//    //            val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)    = fields
//    //            val hashedServiceID = md5SumString(serviceID.getBytes("UTF-8"))
//    //            (id, serviceType, hashedServiceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
//    //    }.project('ProfileID, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng).write(TextLine(out))
//    //
//    //
//    //    def md5SumString(bytes : Array[Byte]) : String = {
//    //        val md5 = MessageDigest.getInstance("MD5")
//    //        md5.reset()
//    //        md5.update(bytes)
//    //        md5.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
//    //    }
//
//}
//
//object CheckinGrouperFunction {
//    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
//}
//
//
//
