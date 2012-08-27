package com.sonar.expedition.scrawler.test

import com.sonar.expedition.scrawler.pipes._
import com.twitter.scalding.{Job, Args, TextLine}
import com.sonar.expedition.scrawler.util.CommonFunctions._

class CheckinGrouperFunctionTest(args: Args) extends Job(args) with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction {
    val serviceProfileInput = args("serviceProfileData")
    val friendsInput = args("friendData")
    val checkinsInput = args("checkinData")
    val checkinTupleExport = args("output")


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
    val chkindata = checkinTuple(TextLine(checkinsInput).read, friends, serviceIds)
            .project('keyid, 'serType, 'hasheduser, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'latitude, 'longitude, 'city, 'numberOfFriendsAtVenue, 'numberOfVenueVisits)
            .write(TextLine(checkinTupleExport))
}
