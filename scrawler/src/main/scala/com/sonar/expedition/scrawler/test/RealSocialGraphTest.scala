package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.jobs.DataAnalyser

class RealSocialGraphTest(args: Args) extends Job(args) {
    val serviceProfileInput = args("serviceProfileData")
    val friendsInput = args("friendData")
    val checkinsInput = args("checkinData")
    val matchedFriends = args("output")

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val friendGrouper = new FriendGrouperFunction(args)

    val data = (TextLine(serviceProfileInput).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case DataAnalyser.ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)

    val friendsNearby = new RealSocialGraph(args)
    val friends = friendGrouper.groupFriends(TextLine(friendsInput).read)
    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid, 'uname)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))
    val chkindata = checkinGrouperPipe.unfilteredCheckins(TextLine(checkinsInput).read)

    val findFriendsAtTheSameVenue = friendsNearby.friendsNearbyByChunks(friends, chkindata, serviceIds)
            .write(TextLine(matchedFriends))


}