package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import util.matching.Regex
import java.util.Calendar
import CheckinGrouperFunction._
import cascading.pipe.joiner.LeftJoin
import java.security.MessageDigest

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
            hour: Double => hour > 8.5 && hour < 18.5
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

    def unfilteredCheckins(input: RichPipe): RichPipe = {


        val data = input
                .mapTo('line ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour)) {
            line: String => {
                line match {
                    case DataExtractLine(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng) => {
                        val timeFilter = Calendar.getInstance()
                        val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                        timeFilter.setTime(checkinDate)
                        //                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        val date = timeFilter.get(Calendar.DAY_OF_YEAR)
                        val time = timeFilter.get(Calendar.HOUR_OF_DAY) + timeFilter.get(Calendar.MINUTE) / 60.0
                        (id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, date, time)
                    }
                    case _ => {
                        println("Coudn't extract line using regex: " + line)
                        ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None", 1, 0.0)
                    }
                }
            }
        }.map(('latitude, 'longitude) -> ('loc)) {
            fields: (String, String) =>
                val (lat, lng) = fields
                val loc = lat + ":" + lng
                (loc)
        }

                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour))

        data
    }

    def checkinTuple(input: RichPipe, friendsInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        var data = input
                .mapTo('line ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour)) {
            line: String => {
                line match {
                    case DataExtractLine(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng) => {
                        val timeFilter = Calendar.getInstance()
                        val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                        timeFilter.setTime(checkinDate)
                        //                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        val date = timeFilter.get(Calendar.DAY_OF_YEAR)
                        val time = timeFilter.get(Calendar.HOUR_OF_DAY) + timeFilter.get(Calendar.MINUTE) / 60.0
                        (id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, date, time)
                    }
                    case _ => {
                        println("Coudn't extract line using regex: " + line)
                        ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None", 1, 0.0)
                    }
                }
            }
        }.map(('latitude, 'longitude) -> ('loc)) {
            fields: (String, String) =>
                val (lat, lng) = fields
                val loc = lat + ":" + lng
                (loc)
        }

                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour))



        val numberOfVenueVisits = addTotalTimesCheckedIn(data).project('keyid, 'numberOfVenueVisits)

        val numberOfFriendsAtVenue = friendsAtSameVenue(friendsInput, data, serviceIdsInput).project('uId, 'numberOfFriendsAtVenue)

        data = data.joinWithSmaller('keyid -> 'uId, numberOfFriendsAtVenue, joiner = new LeftJoin)
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour, 'numberOfFriendsAtVenue)

        addTotalTimesCheckedIn(data).project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'loc, 'numberOfFriendsAtVenue, 'numberOfVenueVisits)
                .map(('loc) ->('lat, 'lng)) {
            fields: (String) =>
                val loc = fields
                val lat = loc.split(":").head
                val long = loc.split(":").last
                (lat, long)
        }.flatMap(('lat, 'lng) ->('latitude, 'longitude, 'city)) {
            fields: (String, String) => {
                val (lat, lng) = fields
                // List(MetroArea(Point(40.0,-73.0),Point(...), "NY"))
                val metroAreas = List(((40.489, -74.327), (40.924, -73.723), "New York"),
                    ((33.708, -118.620), (34.303, -117.780), "Los Angeles"), ((37.596, -122.514), (37.815, -122.362), "San Fransisco"),
                    ((30.139, -97.941), (30.521, -97.568), "Austin"), ((41.656, -87.858), (42.028, -87.491), "Chicago"),
                    ((29.603, -95.721), (29.917, -95.200), "Houston"), ((33.647, -84.573), (33.908, -84.250), "Atlanta"),
                    ((38.864, -94.760), (39.358, -94.371), "Kansas City"), ((30.130, -82.053), (30.587, -81.384), "Jacksonville"))
                val result: Option[((Double, Double), (Double, Double), String)] = metroAreas.find {
                    case ((s, e), (n, w), _) => s >= lat.toDouble && lat.toDouble <= n && e >= lng.toDouble && lng.toDouble <= w
                }
                result map {
                    case (_, _, city) => (lat, lng, city)
                }


            }
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'latitude, 'longitude, 'city, 'numberOfFriendsAtVenue, 'numberOfVenueVisits)
                .map('serProfileID -> 'hasheduser) {
            fields: String =>
                val user = fields
                val hashed = md5SumString(user.getBytes("UTF-8"))
                hashed
        }.project('keyid, 'serType, 'hasheduser, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'latitude, 'longitude, 'city, 'numberOfFriendsAtVenue, 'numberOfVenueVisits)

    }

    def md5SumString(bytes: Array[Byte]): String = {
        val md5 = MessageDigest.getInstance("MD5")
        md5.reset()
        md5.update(bytes)
        md5.digest().map(0xFF & _).map {
            "%02x".format(_)
        }.foldLeft("") {
            _ + _
        }
    }

    def addTotalTimesCheckedIn(input: RichPipe): RichPipe = {
        val counter = input.groupBy('loc) {
            _.size
        }.rename('size -> 'numberOfVenueVisits)
        input.joinWithSmaller('loc -> 'loc, counter)
    }

    def friendsAtSameVenue(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val userIdGroupedFriends = friendsInput.project('userProfileId, 'serviceProfileId, 'friendName)
                .map(('userProfileId, 'serviceProfileId) ->('uId, 'serviceId)) {
            fields: (String, String) =>
                val (userIdString, serviceProfileId) = fields
                val uIdString = userIdString.trim
                val serviceId = serviceProfileId.trim
                (uIdString, serviceId)
        }.project('uId, 'serviceId)

        val findFriendSonarId = serviceIdsInput.project('row_keyfrnd, 'fbId, 'lnId)

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'serviceId, userIdGroupedFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'serviceId, userIdGroupedFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val mergedFriends = linkedinFriends.++(facebookFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val friendsCheckins = checkinInput.joinWithSmaller('keyid -> 'row_keyfrnd, mergedFriends)
                .rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour) ->
                ('friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour))
                .unique('uId, 'friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour)

        val matchingCheckins = checkinInput.joinWithSmaller('keyid -> 'uId, friendsCheckins)
                .filter('venName, 'friendVenName, 'friendDayOfYear, 'dayOfYear, 'friendHour, 'hour) {
            fields: (String, String, Int, Int, Double, Double) =>
                val (originalVenue, friendVenue, friendDay, originalDay, friendHour, originalHour) = fields
                (originalVenue.equalsIgnoreCase(friendVenue)) && (friendDay == originalDay) && (originalHour <= (friendHour + 1.5)) && (originalHour >= (friendHour - 1.5)) && (originalVenue != null) && (friendVenue != "")
        }.project('uId, 'friendKey, 'venName, 'friendVenName, 'dayOfYear, 'friendDayOfYear, 'hour, 'friendHour)
                .groupBy('uId) {
            _.size
        }.rename('size -> 'numberOfFriendsAtVenue).project('uId, 'numberOfFriendsAtVenue)

        matchingCheckins
    }

}

object CheckinGrouperFunction {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.]*)::([\-\d]*)::([\.\d\-E]+)::([\.\d\-E]+)""".r
}