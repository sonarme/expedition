package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.util.CommonFunctions._

/*
inputs : prod exports
--serviceProfileData
--twitterServiceProfileData
--friendData
--checkinData

output : code with matched venues, top three flattened, has venue details and a weight
--output

com.sonar.expedition.scrawler.test.SonarCheckinVenueTest --local --serviceProfileData "/data/serviceProfileData.txt"
--twitterServiceProfileData "/data/twitterserviceProfileData.txt" --friendData "/data/friendData.txt"
--checkinData "/data/checkinData.txt" --output "/tmp/matchedFriends.txt"

 */

class SonarCheckinVenueTest(args: Args) extends Job(args) {

    val checkinsInput = args("checkinData")
    val friendsInput = args("friendData")
    val serviceInput = args("serviceProfileData")
    val twitterServiceProfileInput = args("twitterServiceProfileData")
    val matchedVenues = args("output")

    val data = (TextLine(serviceInput).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val twitterdata = (TextLine(twitterServiceProfileInput).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))




    val dtoProfilePipe = new DTOProfileInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val friendGrouperPipe = new FriendGrouperFunction(args)
    val venueFinder = new SonarCheckinVenue(args)

    val chkindata = checkinGrouperPipe.unfilteredCheckins(TextLine(checkinsInput).read)
    val frienddata = friendGrouperPipe.groupFriends(TextLine(friendsInput).read)
    val joinedProfiles = dtoProfilePipe.getTotalProfileTuples(data, twitterdata)
    val serviceIds = joinedProfiles.rename('key ->'friendkey).project(('friendkey, 'uname, 'fbid, 'lnid, 'twid, 'fsid))

    val sonarVenues = venueFinder.getCheckinVenue(chkindata, frienddata, serviceIds)
            .write(TextLine(matchedVenues))


}