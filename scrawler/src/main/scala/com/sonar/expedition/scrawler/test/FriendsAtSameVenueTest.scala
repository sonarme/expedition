package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.util.CommonFunctions._

class FriendsAtSameVenueTest(args: Args) extends Job(args) {
    val serviceProfileInput = "/tmp/serviceProfileData.txt"
    val friendsInput = "/tmp/friendData.txt"
    val checkinsInput = "/tmp/checkinDatatest.txt"
    val matchedFriends = "/tmp/matchedFriends.txt"

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val friendGrouper = new FriendGrouperFunction(args)

    val data = (TextLine(serviceProfileInput).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)

    val friendsNearby = new FriendsAtSameVenue(args)
    val friends = friendGrouper.groupFriends(TextLine(friendsInput).read)
    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))
    val chkindata = checkinGrouperPipe.unfilteredCheckins(TextLine(checkinsInput).read)

    val findFriendsAtTheSameVenue = friendsNearby.friendsAtSameVenue(friends, chkindata, serviceIds)
            .project(('uId, 'friendKey, 'venName, 'friendVenName, 'dayOfYear, 'friendDayOfYear, 'hour, 'friendHour))
            .write(TextLine(matchedFriends))

}