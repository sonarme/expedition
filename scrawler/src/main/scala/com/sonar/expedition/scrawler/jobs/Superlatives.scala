package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{DTOProfileInfoPipe, CheckinGrouperFunction}
import cascading.tuple.Fields
import DataAnalyser._

class Superlatives(args: Args) extends Job(args) {

    /*
    //analyse profile based on profile info and checkins info

    try to find some interetsing results like
    user having most no of fb frinds
    user having most no of checkins
    user having most number of checkins in chipotle or burger king
    users having most number of common names

     */

    val checkinInfoPipe = new CheckinGrouperFunction(args)
    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)

    val chckininfo = TextLine("/tmp/checkin_nomsg.txt")
    val servprofin = TextLine("/tmp/serviceProfileData.txt")

    val chckininfoout = TextLine("/tmp/checkin")
    val servprofout = TextLine("/tmp/servprof")

    //checkin info
    val chkinpipe = checkinInfoPipe.unfilteredCheckinsLatLon(chckininfo).project(Fields.ALL).write(chckininfoout)
    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour


    //profile info
    val data = (servprofin.read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))
    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoForSuperlativeAnalysis(data)
            //('skey, 'fbuname, 'fid, 'lid, 'edulist, 'worklist, 'citylist,'likeslist)
            .joinWithSmaller('skey -> 'keyid, chkinpipe)
            .project('skey, 'fbuname, 'fid, 'lid, 'edulist, 'worklist, 'citylist, 'likeslist, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour)


    val mostchkins = joinedProfiles.project('skey, 'fbuname, 'venName)
            .groupBy('skey, 'fbuname) {
        _
                .toList[String]('venName -> 'venName1)

    }
            .mapTo(('skey, 'fbuname, 'venName1) ->('skey1, 'fbuname, 'venuesvisited)) {
        fields: (String, String, List[String]) =>
            val (fid, fbname, venues) = fields
            (fid, fbname, venues.size)
    }
            .groupAll {
        _.sortBy('venuesvisited).reverse.take(100)
    }
            .write(servprofout)

}
