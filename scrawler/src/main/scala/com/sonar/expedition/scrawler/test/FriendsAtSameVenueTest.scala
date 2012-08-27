package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.jobs.Job

class FriendsAtSameVenueTest(args: Args) extends Job(args) with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with FriendsAtSameVenue {
    val serviceProfileInput = args("serviceProfileData")
    val friendsInput = args("friendData")
    val checkinsInput = args("checkinData")
    val matchedFriends = args("matchedFriends")

    val data = (TextLine(serviceProfileInput).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val joinedProfiles = getDTOProfileInfoInTuples(data)

    val friends = groupFriends(TextLine(friendsInput).read)
    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))
    val chkindata = unfilteredCheckins(TextLine(checkinsInput).read)

    val findFriendsAtTheSameVenue = friendsAtSameVenueDuplicate(friends, chkindata, serviceIds)
            .project(('uId, 'friendKey, 'venName, 'friendVenName, 'dayOfYear, 'friendDayOfYear, 'hour, 'friendHour))
            .write(TextLine(matchedFriends))

}
