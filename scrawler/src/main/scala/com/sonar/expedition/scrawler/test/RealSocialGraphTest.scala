package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{SequenceFile, TextLine, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.util.CommonFunctions._


/*
inputs : prod exports
--serviceProfileData
--twitterServiceProfileData
--friendData
--checkinData

output : code with matched friends in real social graph, will have ('keyid, 'keyid2, 'uname, 'uname2, 'size)
--output

com.sonar.expedition.scrawler.test.RealSocialGraphTest --local --serviceProfileData "/data/serviceProfileData.txt"
--twitterServiceProfileData "/data/twitterserviceProfileData.txt" --friendData "/data/friendData.txt"
--checkinData "/data/checkinData.txt" --output "/tmp/matchedFriends.txt"

*/

class RealSocialGraphTest(args: Args) extends DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with RealSocialGraph {
    val serviceProfileInput = args("serviceProfileData")
    val friendsInput = args("friendData")
    val checkinsInput = args("checkinData")
    val matchedFriends = args("output")


    val joinedProfiles = getTotalProfileTuples(args)

    val friends = SequenceFile(friendsInput, FriendTuple).read
    val serviceIds = joinedProfiles.rename('key -> 'friendkey).project(('friendkey, 'uname, 'fbid, 'lnid, 'twid, 'fsid))
    val chkindata = null //TODO: unfilteredCheckins(TextLine(checkinsInput).read)

    val findFriendsAtTheSameVenue = friendsNearbyByFriends(friends, chkindata, serviceIds)
            .write(TextLine(matchedFriends))


}
