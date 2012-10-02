package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile


class ExtractCentroids(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {


    val friendinput = args("friendInput")

    val (newCheckinsX, _) = checkinSource(args, false, false)
    val newCheckins = unfilteredCheckinsFromCassandra(newCheckinsX)
    val total = serviceProfiles(args)
    /*

        val profiles = ageEducationPipe(total)
                .discard('key)
                .flatMap(('fbid, 'lnid, 'fsid, 'twid) -> 'key) {
            in: (String, String, String, String) =>
                val (fbid, lnid, fsid, twid) = in
                //nned not handle linked in because there ar no checkins from linked in and sonar checkins dont have id , so key comes as sonar: empty, need to fix it, ask Paul, todo.
                List("facebook:" + fbid, "twitter:" + twid, "foursquare:" + fsid)
        }.groupBy('key) {
            _.head('uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked)
        }
    */

    val chkindata = groupCheckins(newCheckins)

    val friendsForCoworker = SequenceFile(friendinput, FriendTuple).read

    val profilesAndCheckins = newCheckins.map('keyid -> 'key) {
        identity[String]
    }.project('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

    val employerGroupedServiceProfiles = total.project('key, 'worked)

    val serviceIds = total.project('key, 'fbid, 'lnid).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val coworkerCheckins = findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)

    val workCentroids = findClusteroidofUserFromChkins(profilesAndCheckins ++ coworkerCheckins)

    val homeCheckins = groupHomeCheckins(newCheckins)

    val homeCentroids = findClusteroidofUserFromChkins(homeCheckins.map('keyid -> 'key) {
        identity[String]
    })

    homeCentroids.rename('centroid -> 'homeCentroid)
            .leftJoinWithSmaller('key -> 'key1, workCentroids.rename(('key, 'centroid) ->('key1, 'workCentroid))).discard('key1)
            .write(SequenceFile(args("output"), ('key, 'workCentroid, 'homeCentroid)))

}
