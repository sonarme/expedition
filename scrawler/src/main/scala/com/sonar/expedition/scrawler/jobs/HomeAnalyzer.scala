package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.util._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.apis.APICalls
import cascading.pipe.joiner.LeftJoin
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.TextLine
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine

class HomeAnalyzer(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")

    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)

    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)

    val chkindata = checkinGrouperPipe.groupHomeCheckins(TextLine(chkininputData).read)

    val profilesAndCheckins = joinedProfiles.joinWithLarger('key -> 'keyid, chkindata).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins)

    joinedProfiles.joinWithSmaller('key -> 'key1, findcityfromchkins).project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'centroid))
            .map('worked -> 'work) {
        fields: (String) =>
            var (worked) = fields
            if (worked == null || worked == "") {
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
