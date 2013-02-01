package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.RichPipe
import java.util.Date
import org.joda.time._
import com.sonar.expedition.scrawler.checkins.CheckinInference
import com.sonar.expedition.common.segmentation.TimeSegmentation


trait CheckinGrouperFunction extends ScaldingImplicits with CheckinInference with TimeSegmentation {
    def workCheckins(input: RichPipe) =
        input.filter('dayOfWeek, 'hour) {
            fields: (Int, Int) =>
                val (dayOfWeek, hour) = fields
                dayOfWeek >= DateTimeConstants.MONDAY && dayOfWeek <= DateTimeConstants.FRIDAY && hour >= 10 && hour <= 16
        }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)


    def homeCheckins(input: RichPipe) =
        input.filter('dayOfWeek, 'hour) {
            fields: (Int, Int) =>
                val (dayOfWeek, hour) = fields
                dayOfWeek == DateTimeConstants.SATURDAY || dayOfWeek == DateTimeConstants.SUNDAY || hour <= 7 || hour >= 20
        }.project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))


    def deriveCheckinFields(lat: Double, lng: Double, checkinTime: Date, serviceType: String, serviceProfileId: String) = {
        val ldt = localDateTime(lat, lng, checkinTime)
        val goldenId = serviceType + ":" + serviceProfileId
        (ldt.getDayOfYear, ldt.getDayOfWeek, ldt.getHourOfDay, goldenId)
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
