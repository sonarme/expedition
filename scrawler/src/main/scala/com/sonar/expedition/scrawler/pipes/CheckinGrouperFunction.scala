package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import util.matching.Regex
import java.util.Calendar
import CheckinGrouperFunction._
import cascading.pipe.joiner.LeftJoin
import java.security.MessageDigest
import ch.hsr.geohash.{WGS84Point, BoundingBox}
import com.sonar.expedition.scrawler.util.CommonFunctions._

class CheckinGrouperFunction(args: Args) extends Job(args) {


    def groupCheckins(input: RichPipe): RichPipe = {

        val data = unfilteredCheckins(input)
                .filter('dayOfWeek) {
            dayOfWeek: Int => dayOfWeek > 1 && dayOfWeek < 7
        }.filter('hour) {
            hour: Double => hour > 8 && hour < 22 //user may checkin in 9-10 p.m for dinner
        }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)


        data
    }

    def groupHomeCheckins(input: RichPipe): RichPipe = {

        val data = unfilteredCheckins(input)
                .filter('dayOfWeek, 'hour) {
            fields: (Int, Double) =>
                val (dayOfWeek, hour) = fields
                ((dayOfWeek == 1 || dayOfWeek == 7) || (dayOfWeek > 1 && dayOfWeek < 7 && (hour < 8.5 || hour > 18.5)))
        }.project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

        data
    }

    def unfilteredCheckins(input: RichPipe): RichPipe = {

        val data = input
                .flatMapTo('line ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'dayOfWeek, 'hour)) {
            line: String => {
                line match {
                    case CheckinExtractLineWithMessages(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, msg) => {
                        val timeFilter = Calendar.getInstance()
                        val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                        timeFilter.setTime(checkinDate)
                        val date = timeFilter.get(Calendar.DAY_OF_YEAR)
                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        val time = timeFilter.get(Calendar.HOUR_OF_DAY) + timeFilter.get(Calendar.MINUTE) / 60.0 + timeFilter.get(Calendar.SECOND) / 3600.0
                        Some((id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, date, dayOfWeek, time))
                    }
                    case CheckinExtractLine(id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng) => {
                        val timeFilter = Calendar.getInstance()
                        val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                        timeFilter.setTime(checkinDate)
                        val date = timeFilter.get(Calendar.DAY_OF_YEAR)
                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        val time = timeFilter.get(Calendar.HOUR_OF_DAY) + timeFilter.get(Calendar.MINUTE) / 60.0 + timeFilter.get(Calendar.SECOND) / 3600.0
                        Some((id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, date, dayOfWeek, time))
                    }
                    case _ => {
                        println("Coudn't extract line using regex: " + line)
                        None
                    }
                }
            }
        }
                .map(('latitude, 'longitude) -> ('loc)) {
            fields: (String, String) =>
                val (lat, lng) = fields
                val loc = lat + ":" + lng
                (loc)
        }

                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'dayOfWeek, 'hour))

        data
    }

    def correlationCheckins(input: RichPipe): RichPipe = {

        val data = input
                .flatMapTo('line ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour)) {
            line: String => {
                line match {
                    case CheckinExtractLineWithVenueId(golden, id, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, checkinTime, geoHash, lat, lng, venueId, msg) => {
                        val timeFilter = Calendar.getInstance()
                        val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                        timeFilter.setTime(checkinDate)
                        val date = timeFilter.get(Calendar.DAY_OF_YEAR)
                        val dayOfWeek = timeFilter.get(Calendar.DAY_OF_WEEK)
                        val time = timeFilter.get(Calendar.HOUR_OF_DAY) + timeFilter.get(Calendar.MINUTE) / 60.0 + timeFilter.get(Calendar.SECOND) / 3600.0
                        val goldenId = golden + ":" + id
                        Some((goldenId, serviceType, serviceId, serviceCheckinId, venueName, venueAddress, venueId, checkinTime, geoHash, lat, lng, date, dayOfWeek, time))
                    }
                    case _ => {
                        println("Coudn't extract line using regex: " + line)
                        None
                    }
                }
            }
        }
                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour))

        data
    }

    def unfilteredCheckinsLatLon(input: RichPipe): RichPipe = {

        val data = unfilteredCheckins(input)
                .map(('loc) ->('lat, 'lng)) {
            fields: (String) =>
                val loc = fields
                val lat = loc.split(":").head
                val long = loc.split(":").last
                (lat, long)
        }
                .discard('loc)

        data

    }

    def checkinTuple(input: RichPipe, friendsInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val checkins = unfilteredCheckins(input)

        val numberOfVenueVisits = addTotalTimesCheckedIn(checkins).project(('keyid, 'numberOfVenueVisits))

        val numberOfFriendsAtVenue = friendsAtSameVenue(friendsInput, checkins, serviceIdsInput).project(('uId, 'numberOfFriendsAtVenue))

        val data = checkins.joinWithSmaller('keyid -> 'uId, numberOfFriendsAtVenue, joiner = new LeftJoin)
                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour, 'numberOfFriendsAtVenue))

        addTotalTimesCheckedIn(data).project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'loc, 'numberOfFriendsAtVenue, 'numberOfVenueVisits))
                .map(('loc) ->('lat, 'lng)) {
            fields: (String) =>
                val loc = fields
                val lat = loc.split(":").head
                val long = loc.split(":").last
                (lat, long)
        }.flatMap(('lat, 'lng) ->('latitude, 'longitude, 'city)) {
            fields: (String, String) => {
                val (latField, lngField) = fields
                if (latField == "None" || lngField == "None") None
                else {
                    val lat = latField.toDouble
                    val lng = lngField.toDouble
                    val locPoint: WGS84Point = new WGS84Point(lat, lng)
                    // List(MetroArea(Point(40.0,-73.0),Point(...), "NY"))
                    val metroAreas = List((new BoundingBox(new WGS84Point(40.489, -74.327), new WGS84Point(40.924, -73.723)), "New York"),
                        (new BoundingBox(new WGS84Point(33.708, -118.620), new WGS84Point(34.303, -117.780)), "Los Angeles"), (new BoundingBox(new WGS84Point(37.596, -122.514), new WGS84Point(37.815, -122.362)), "San Fransisco"),
                        (new BoundingBox(new WGS84Point(30.139, -97.941), new WGS84Point(30.521, -97.568)), "Austin"), (new BoundingBox(new WGS84Point(41.656, -87.858), new WGS84Point(42.028, -87.491)), "Chicago"),
                        (new BoundingBox(new WGS84Point(29.603, -95.721), new WGS84Point(29.917, -95.200)), "Houston"), (new BoundingBox(new WGS84Point(33.647, -84.573), new WGS84Point(33.908, -84.250)), "Atlanta"),
                        (new BoundingBox(new WGS84Point(38.864, -94.760), new WGS84Point(39.358, -94.371)), "Kansas City"), (new BoundingBox(new WGS84Point(30.130, -82.053), new WGS84Point(30.587, -81.384)), "Jacksonville"))
                    val result: Option[(BoundingBox, String)] = metroAreas.find {
                        case (boundingBox: BoundingBox, _) => boundingBox.contains(locPoint)
                    }
                    result map {
                        case (_, city) => (locPoint.getLatitude, locPoint.getLongitude, city)
                    }
                }


            }
        }
                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'latitude, 'longitude, 'city, 'numberOfFriendsAtVenue, 'numberOfVenueVisits))
                .map('serProfileID -> 'hasheduser) {
            fields: String =>
                val user = fields
                val hash = hashed(user)
                hash
        }.project(('keyid, 'serType, 'hasheduser, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'latitude, 'longitude, 'city, 'numberOfFriendsAtVenue, 'numberOfVenueVisits))

    }


    def addTotalTimesCheckedIn(input: RichPipe): RichPipe = {
        val counter = input.groupBy('loc) {
            _.size
        }.rename('size -> 'numberOfVenueVisits)
        input.joinWithSmaller('loc -> 'loc, counter)
    }

    def friendsAtSameVenue(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val userIdGroupedFriends = friendsInput.project(('userProfileId, 'serviceProfileId, 'friendName))
                .map(('userProfileId, 'serviceProfileId) ->('uId, 'serviceId)) {
            fields: (String, String) =>
                val (userIdString, serviceProfileId) = fields
                val uIdString = userIdString.trim
                val serviceId = serviceProfileId.trim
                (uIdString, serviceId)
        }.project(('uId, 'serviceId))

        val findFriendSonarId = serviceIdsInput.project(('row_keyfrnd, 'fbId, 'lnId))

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'serviceId, userIdGroupedFriends)
                .project(('uId, 'row_keyfrnd, 'fbId, 'lnId))

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'serviceId, userIdGroupedFriends)
                .project(('uId, 'row_keyfrnd, 'fbId, 'lnId))

        val mergedFriends = linkedinFriends.++(facebookFriends)
                .project(('uId, 'row_keyfrnd, 'fbId, 'lnId))

        val friendsCheckins = checkinInput.joinWithSmaller('keyid -> 'row_keyfrnd, mergedFriends)
                .rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour) ->
                ('friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour))
                .unique(('uId, 'friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour))

        val matchingCheckins = checkinInput.joinWithSmaller('keyid -> 'uId, friendsCheckins)
                .filter(('venName, 'friendVenName, 'friendDayOfYear, 'dayOfYear, 'friendHour, 'hour)) {
            fields: (String, String, Int, Int, Double, Double) =>
                val (originalVenue, friendVenue, friendDay, originalDay, friendHour, originalHour) = fields
                (originalVenue.equalsIgnoreCase(friendVenue)) && (friendDay == originalDay) && (originalHour <= (friendHour + 1.5)) && (originalHour >= (friendHour - 1.5)) && (originalVenue != null) && (friendVenue != "")
        }.project(('uId, 'friendKey, 'venName, 'friendVenName, 'dayOfYear, 'friendDayOfYear, 'hour, 'friendHour))
                .groupBy('uId) {
            _.size
        }.rename('size -> 'numberOfFriendsAtVenue).project(('uId, 'numberOfFriendsAtVenue))

        matchingCheckins
    }

}

object CheckinGrouperFunction {
}