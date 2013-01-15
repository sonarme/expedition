/*package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Args}
import com.sonar.expedition.scrawler.pipes.{DTOProfileInfoPipe, CheckinGrouperFunction}
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.CommonFunctions._

class Superlatives(args: Args) extends DefaultJob(args) {

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

    val mostjobexperiencedout = TextLine("/tmp/mostjobexperienced")
    val mosteducatedout = TextLine("/tmp/mosteducatedout")
    val mostjobexperiencedgeohashout = TextLine("/tmp/mostjobexperiencedgeohashout")
    val mostjobexperiencedgeohashout2 = TextLine("/tmp/mostjobexperiencedgeohashout2")
    val mostjobexperiencedgeohashout3 = TextLine("/tmp/mostjobexperiencedgeohashout3")

    //checkin info
    val chkinpipe = checkinInfoPipe.unfilteredCheckinsLatLon(chckininfo).project(Fields.ALL).write(chckininfoout)
    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour


    //profile info
    val data = (servprofin.read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoForSuperlativeAnalysis(data)
            //('skey, 'fbuname, 'fid, 'lid, 'edulist, 'worklist, 'citylist,'likeslist)
            .joinWithSmaller('skey -> 'keyid, chkinpipe)
            .project(('skey, 'fbuname, 'fid, 'lid, 'edulist, 'worklist, 'citylist, 'likeslist, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour))


    val mostchkins = joinedProfiles.project(('skey, 'fbuname, 'venName))
            .groupBy(('skey, 'fbuname)) {
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

    val mosteducated = joinedProfiles.project(('skey, 'edulist))
            .mapTo(('skey, 'edulist) -> (('skey1, 'edulistsize))) {
        fields: (String, List[String]) =>
            val (skey, list) = fields
            (skey, list.size)
    }.groupBy('skey1) {
        _
                .toList[Int]('edulistsize, 'edulistsize1)
    }.mapTo(('skey1, 'edulistsize1) ->('skey2, 'edulistsize2)) {
        fields: (String, List[Int]) =>
            val (skey, list) = fields

            (skey, list.max)

    }.unique(('skey2, 'edulistsize2)).groupAll {
        _.sortBy('edulistsize2).reverse.take(100)
    }.write(mosteducatedout)

    val mostjobexperienced = joinedProfiles.project(('skey, 'worklist))
            .mapTo(('skey, 'worklist) -> (('skey1, 'worklistsize))) {
        fields: (String, List[String]) =>
            val (skey, list) = fields
            (skey, list.size)
    }.groupBy('skey1) {
        _
                .toList[Int]('worklistsize, 'worklistsize1)
    }.mapTo(('skey1, 'worklistsize1) ->('skey2, 'worklistsize2)) {
        fields: (String, List[Int]) =>
            val (skey, list) = fields

            (skey, list.max)
    }.unique(('skey2, 'worklistsize2)).groupAll {
        _.sortBy('worklistsize2).reverse
    }.write(mostjobexperiencedout)


    val mostjobexperiencedgeohash = joinedProfiles.project(('skey, 'worklist, 'ghash))
            .mapTo(('skey, 'worklist, 'ghash) -> (('skey1, 'worklistsize, 'ghash1))) {
        fields: (String, List[String], String) =>
            val (skey, list, geo) = fields
            (skey, list.size, geo)
    }

    val mostjobexperiencedgeohash1 = mostjobexperiencedgeohash.groupBy('skey1) {
        _
                .toList[Int]('worklistsize, 'worklistsize1)


    }.mapTo(('skey1, 'worklistsize1) ->('skey2, 'worklistsize2)) {
        fields: (String, List[Int]) =>
            val (skey, list) = fields
            (skey, list.max)
    }
            .joinWithSmaller('skey2 -> 'skey1, mostjobexperiencedgeohash)
            .project(('skey2, 'worklistsize2, 'ghash1))
            .unique(('skey2, 'worklistsize2, 'ghash1)).groupAll {
        _.sortBy('worklistsize2).reverse
    }.write(mostjobexperiencedgeohashout)


    val mostjobexperiencedgeohash2 = mostjobexperiencedgeohash1.groupBy('ghash1) {
        _
                .toList[String]('skey2, 'skey3)
                .toList[Int]('worklistsize2, 'worklistsize3)
    }
            .mapTo(('ghash1, 'skey3, 'worklistsize3) ->('ghash2, 'highestskey, 'highestworklistsize3)) {
        fields: (String, List[String], List[Int]) =>
            val (ghash, userKeys, numberOfJobsPerPerson) = fields
            val (highestUserKey, highestNumberOfJobsForUser) = (userKeys zip numberOfJobsPerPerson) maxBy (_._2)
            (ghash, highestUserKey, highestNumberOfJobsForUser)
    }.groupAll {
        _.sortBy('highestworklistsize3).reverse
    }
            .write(mostjobexperiencedgeohashout2)

    val mostjobexperiencedgeohash3 = mostjobexperiencedgeohash1.mapTo(('skey2, 'worklistsize2, 'ghash1) ->('skey3, 'worklistsize3, 'ghashtrimmed3)) {
        fields: (String, String, String) =>
            val (key, worklistsize, geohash) = fields

            (key, worklistsize, geohash.substring(0, geohash.length - 2))

    }.rename(('skey3, 'worklistsize3, 'ghashtrimmed3) ->('skey2, 'worklistsize2, 'ghash1)).groupBy('ghash1) {
        _
                .toList[String]('skey2, 'skey3)
                .toList[Int]('worklistsize2, 'worklistsize3)
    }
            .mapTo(('ghash1, 'skey3, 'worklistsize3) ->('ghash2, 'highestskey, 'highestworklistsize3)) {
        fields: (String, List[String], List[Int]) =>
            val (ghash, userKeys, numberOfJobsPerPerson) = fields
            val (highestUserKey, highestNumberOfJobsForUser) = (userKeys zip numberOfJobsPerPerson) maxBy (_._2)
            (ghash, highestUserKey, highestNumberOfJobsForUser)
    }.groupAll {
        _.sortBy('highestworklistsize3).reverse
    }
            .write(mostjobexperiencedgeohashout3)


}
*/
