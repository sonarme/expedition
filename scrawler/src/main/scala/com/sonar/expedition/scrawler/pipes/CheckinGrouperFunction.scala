package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import util.matching.Regex
import java.util.Calendar
import CheckinGrouperFunction._

class CheckinGrouperFunction(args: Args) extends Job(args) {


    def groupCheckins(input: RichPipe): RichPipe = {


        val data = input
                .mapTo('line ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfWeek, 'hour)) {
            line: String => {
                line match {
                    case DataExtractLine(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng) => {
                        val timeFilter = Calendar.getInstance()
                        val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                        timeFilter.setTime(checkinDate)
                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        val time = timeFilter.get(Calendar.HOUR_OF_DAY) + timeFilter.get(Calendar.MINUTE) / 60.0
                        (id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, dayOfWeek, time)
                    }
                    case _ => {
                        println("Coudn't extract line using regex: " + line)
                        ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None", 1, 0.0)
                    }
                }
            }
        }
                .filter('dayOfWeek) {
            dayOfWeek: Int => dayOfWeek > 1 && dayOfWeek < 7
        }.filter('hour) {
            hour: Double => hour > 8.5 && hour < 18
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
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin)::([\w\d\-\.@]*)::([\w\d]+)::(.*?)::(.*?)::([\d\-:T\+]*)::([\-\d]*)::([\.\d\-]+)::([\.\d\-]+)""".r
}