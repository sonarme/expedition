package com.sonar.expedition.scrawler.test

import com.twitter.scalding._
import com.sonar.expedition.scrawler.util._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.apis.APICalls
import cascading.pipe.joiner.LeftJoin
import com.sonar.expedition.scrawler.util.CommonFunctions._
import cascading.tuple.Fields
import com.twitter.scalding.TextLine

// JUST FOR TESTING
class HomeAnalyzer(args: Args) extends Job(args) with DTOProfileInfoPipe with CheckinGrouperFunction with CheckinInfoPipe {

    val inputData = args("serviceProfileData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")


    val joinedProfiles = getTotalProfileTuples(args)

    val chkindata = groupHomeCheckins(TextLine(chkininputData).read)

    val profilesAndCheckins = joinedProfiles.joinWithLarger('key -> 'keyid, chkindata).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val findcityfromchkins = findClusteroidofUserFromChkins(profilesAndCheckins)

    joinedProfiles.joinWithSmaller('key -> 'key1, findcityfromchkins).project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'centroid))
            .map('worked -> 'work) {
        fields: (String) =>
            var (worked) = fields
            if (worked == null || worked.isEmpty) {
                worked = " "
            }
            worked
    }
            .map('centroid ->('lat, 'long)) {
        fields: String =>
            val (centroid) = fields
            val latLongArray = centroid.split(":")
            val lat = latLongArray.head
            val long = latLongArray.last
            (lat, long)
    }
            .project('key, 'uname, 'fbid, 'lnid, 'city, 'lat, 'long)
            .write(TextLine(jobOutput))

}


object HomeAnalyzer {

}
