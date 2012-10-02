package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichDate, RichPipe, Args}
import util.matching.Regex
import cascading.pipe.joiner.LeftJoin
import java.security.MessageDigest
import ch.hsr.geohash.{GeoHash, WGS84Point, BoundingBox}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import java.util.{Date, TimeZone, Calendar}
import org.scalastuff.scalabeans.types.DateType
import org.joda.time.{LocalDateTime, DateTimeFieldType, LocalDate, DateTime}
import com.sonar.expedition.scrawler.util.TimezoneLookup


trait CheckinGrouperFunction extends ScaldingImplicits {
    def groupCheckins(input: RichPipe) = input.filter('dayOfWeek) {
        dayOfWeek: Int => dayOfWeek > 1 && dayOfWeek < 7
    }.filter('hour) {
        hour: Double => hour > 8 && hour < 22 //user may checkin in 9-10 p.m for dinner
    }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)


    def groupHomeCheckins(input: RichPipe) = input
            .filter('dayOfWeek, 'hour) {
        fields: (Int, Double) =>
            val (dayOfWeek, hour) = fields
            ((dayOfWeek == 1 || dayOfWeek == 7) || (dayOfWeek > 1 && dayOfWeek < 7 && (hour < 8.5 || hour > 18.5)))
    }.project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))


    def deriveCheckinFields(lat: Double, lng: Double, checkinTime: Date, serviceType: String, serviceProfileId: String) = {
        val localTz = TimezoneLookup.getClosestTimeZone(lat, lng)
        val localDateTime = new LocalDateTime(checkinTime, localTz)
        val goldenId = serviceType + ":" + serviceProfileId
        (localDateTime.dayOfYear(), localDateTime.dayOfWeek(), localDateTime.hourOfDay(), goldenId)
    }

    def unfilteredCheckinsFromCassandra(input: RichPipe): RichPipe = {
        input.map(('lat, 'lng) -> ('loc)) {
            fields: (Double, Double) =>
                val (lat, lng) = fields
                val loc = lat.toString + ":" + lng.toString
                (loc)
        }
    }


    def friendsAtSameVenue(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val userIdGroupedFriends = friendsInput.project(('keyid, 'serviceProfileId))
                .map(('keyid, 'serviceProfileId) ->('uId, 'serviceId)) {
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
