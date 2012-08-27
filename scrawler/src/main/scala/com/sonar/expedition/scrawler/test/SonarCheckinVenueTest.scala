package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.jobs.Job

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

class SonarCheckinVenueTest(args: Args) extends Job(args) with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with SonarCheckinVenue {

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


    val chkindata = unfilteredCheckins(TextLine(checkinsInput).read)
    val frienddata = groupFriends(TextLine(friendsInput).read)
    val joinedProfiles = getTotalProfileTuples(data, twitterdata)
    val serviceIds = joinedProfiles.rename('key -> 'friendkey).project(('friendkey, 'uname, 'fbid, 'lnid, 'twid, 'fsid))

    val sonarVenues = getCheckinVenue(chkindata, frienddata, serviceIds)
            .write(TextLine(matchedVenues))


}
