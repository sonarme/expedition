package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouper._
import util.matching.Regex
import grizzled.slf4j.Logging

class CheckinGrouper(args: Args) extends Job(args) {


    // reads in checkin data then packs them into checkinobjects, grouped by sonar id. objects will need to be joined with service profile data and friend data
    var inputData = "/tmp/checkinData.txt"
    var out = "/tmp/userGroupedCheckins.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinId, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
        line: String => {
            line match {
                case CheckinGrouperFunction.DataExtractLine(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng) => (id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                case _ => ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None")
            }
        }
    }) /*.pack[CheckinObjects](('serviceType, 'serviceProfileId, 'serviceCheckinId, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude) -> 'checkin).groupBy('userProfileId) {
        group => group.toList[CheckinObjects]('checkin,'checkindata)
    }.map(Fields.ALL -> ('ProfileId, 'venue)){
        fields : (String,List[CheckinObjects]) =>
            val (userid ,checkin) = fields
            val venue = checkin.map(_.getVenueName)
            (userid,venue)
    }.project('ProfileId,'venue).write(TextLine(out))*/
            .project(('userProfileId, 'venueName, 'latitude, 'longitude)).write(TextLine(out))


    //     .project(Fields.ALL).discard(0).map(Fields.ALL -> ('ProfileId, 'serType, 'serProfileId, 'serCheckinId, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng)){
    //        fields : (String, String, String, String, String, String, String, String, String, String) =>
    //            val (id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng)    = fields
    //            val hashedServiceId = md5SumString(serviceId.getBytes("UTF-8"))
    //            (id, serviceType, hashedServiceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng)
    //    }.project('ProfileId, 'serType, 'serProfileId, 'serCheckinId, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng).write(TextLine(out))
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

}



