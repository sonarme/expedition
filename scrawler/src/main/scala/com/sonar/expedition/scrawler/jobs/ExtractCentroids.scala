package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile


class ExtractCentroids(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {


    val friendinput = args("friendInput")

    val (newCheckins, checkinsWithGoldenId) = checkinSource(args, false, true)

    val checkinsWithGoldenIdAndLoc = checkinsWithGoldenId
            .map(('lat, 'lng) -> 'loc) {
        fields: (String, String) =>
            val (lat, lng) = fields
            lat + ":" + lng
    }

    val total = getTotalProfileTuples(args).map('uname ->('impliedGender, 'impliedGenderProb)) {
        name: String =>
            val (gender, prob) = GenderFromNameProbability.gender(name)
            (gender, prob)
    }

    val profiles = ageEducationPipe(total)
            .discard('key)
            .flatMap(('fbid, 'lnid, 'fsid, 'twid) -> 'key) {
        in: (String, String, String, String) =>
            val (fbid, lnid, fsid, twid) = in
            //nned not handle linked in because there ar no checkins from linked in and sonar checkins dont have id , so key comes as sonar: empty, need to fix it, ask Paul, todo.
            List("facebook:" + fbid, "twitter:" + twid, "foursquare:" + fsid)
    }.groupBy('key) {
        _.head('uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree)
    }

    /*
val joinedProfiles = profiles.rename('key->'rowkey)
val trainer = new BayesModelPipe(args)

val seqModel = SequenceFile(bayesmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

}


val jobtypes = joinedProfiles.rename('worktitle -> 'data)

val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('key, 'weight) ->('income, 'weight1))

val profilesWithIncome = joinedProfiles.joinWithSmaller('worktitle -> 'data, trained).project(('rowkey, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree, 'income))
 .rename('rowkey -> 'key) */


    val combined = combineCheckinsProfiles(checkinsWithGoldenIdAndLoc, profiles)

    val chkindata = groupCheckins(newCheckins)

    val friendsForCoworker = SequenceFile(friendinput, FriendTuple).read

    val profilesAndCheckins = combined.project('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

    val employerGroupedServiceProfiles = total.project('key, 'worked)

    val serviceIds = total.project('key, 'fbid, 'lnid).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val coworkerCheckins = findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)

    val findcityfromchkins = findClusteroidofUserFromChkins(profilesAndCheckins ++ coworkerCheckins)

    val homeCheckins = groupHomeCheckins(newCheckins)

    val homeProfilesAndCheckins = profiles
            .joinWithLarger('key -> 'keyid, homeCheckins)
            .project('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

    val findhomefromchkins = findClusteroidofUserFromChkins(homeProfilesAndCheckins)

    findcityfromchkins.rename(('key1, 'centroid) ->('key, 'workCentroid))
            .leftJoinWithSmaller('key -> 'key1, findhomefromchkins.rename('centroid -> 'homeCentroid))
            .write(SequenceFile(args("output"), ('key, 'workCentroid, 'homeCentroid)))

}
