package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Job, Args}

class FriendsAtSameVenue(args: Args) extends Job(args) {

    def friendsAtSameVenue(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val userIdGroupedFriends = friendsInput.project('userProfileId, 'serviceProfileId, 'friendName)
                .map(('userProfileId, 'serviceProfileId) -> ('uId, 'serviceId)) {
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

        matchingCheckins
    }

}
