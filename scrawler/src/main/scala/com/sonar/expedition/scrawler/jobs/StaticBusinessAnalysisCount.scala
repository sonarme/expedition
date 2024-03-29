package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import cascading.tuple.Fields
import com.twitter.scalding.TextLine

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisCount(args: Args) extends DefaultJob(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation {
    val rpcHostArg = args("rpcHost")

    val friendinput = args("friendInput")
    val bayesmodel = args("bayesmodelforsalary")

    val checkinOut = args("checkinOut")
    val correlationCheckinOut = args("correlationCheckinOut")
    val withHomeWorkOut = args("withHomeWorkOut")
    val combinedOut = args("combinedOut")


    val (checkins, checkinsWithGoldenId) = checkinSource(args, false, true)

    checkins
            .groupAll {
        _.size
    }
            .write(TextLine(checkinOut))
    val checkinsWithGolden = checkinsWithGoldenId

            .map(('lat, 'lng) -> ('loc)) {
        fields: (String, String) =>
            val (lat, lng) = fields
            val loc = lat + ":" + lng
            (loc)
    }

    checkinsWithGolden
            .groupAll {
        _.size
    }
            .write(TextLine(correlationCheckinOut))

    val total = getTotalProfileTuples(args).map('uname ->('impliedGender, 'impliedGenderProb)) {
        name: String => GenderFromNameProbability.gender(name)
    }

    val profiles = ageEducationPipe(total)
            .project(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree))

    val combined = combineCheckinsProfiles(checkinsWithGolden, profiles)

    combined
            .groupAll {
        _.size
    }
            .write(TextLine(combinedOut))


    val chkindata = workCheckins(checkins)
    val friendsForCoworker = SequenceFile(friendinput, FriendTuple).read
    val profilesAndCheckins = combined.project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))
    val employerGroupedServiceProfiles = total.project(('key, 'worked))
    val serviceIds = total.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))
    val coworkerCheckins = findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)
    val findcityfromchkins = findClusterCenter(profilesAndCheckins ++ coworkerCheckins)
    val home = homeCheckins(checkins)
    val homeProfilesAndCheckins = profiles.joinWithLarger('key -> 'keyid, home).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))
    val findhomefromchkins = findClusterCenter(homeProfilesAndCheckins)
    val withHomeWork = combined.joinWithSmaller('key -> 'key1, findcityfromchkins)
            .map('centroid -> 'workCentroid) {
        centroid: String => centroid
    }
            .discard(('key1, 'centroid))
            .joinWithSmaller('key -> 'key1, findhomefromchkins)
            .map('centroid -> 'homeCentroid) {
        centroid: String => centroid
    }

    withHomeWork
            .groupAll {
        _.size
    }
            .write(TextLine(withHomeWorkOut))


}
