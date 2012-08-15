package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import com.twitter.scalding.TextLine
import cascading.tuple.Fields

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTap(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")

    val input = args("serviceProfileInput")
    val twinput = args("twitterServiceProfileInput")
    val checkininput = args("checkinInput")
    val friendinput = args("friendInput")
    val bayestrainingmodel = args("bayestrainingmodelforsalary")

    val data = (TextLine(input).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val twdata = (TextLine(twinput).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val ageEducationPipe = new AgeEducationPipe(args)
    val checkinGroup = new CheckinGrouperFunction(args)
    val friendGroup = new FriendGrouperFunction(args)
    val businessGroup = new BusinessGrouperFunction(args)
    val ageEducation = new AgeEducationPipe(args)
    val reachLoyalty = new ReachLoyaltyAnalysis(args)
    val coworkerPipe = new CoworkerFinderFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)





    val checkins = checkinGroup.unfilteredCheckins(TextLine(checkininput))

    val total = dtoProfileGetPipe.getTotalProfileTuples(data, twdata).map('uname ->('impliedGender, 'impliedGenderProb)) {
        name: String => GenderFromNameProbability.gender(name)
    }

    val profiles = ageEducation.ageEducationPipe(total)
            .project(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree))



    val joinedProfiles = profiles.rename('key->'rowkey)

    val trainer = new BayesModelPipe(args)
    val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
        fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

    }


    val jobtypes = joinedProfiles.rename('worktitle -> 'data)

    val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('key, 'weight) ->('income, 'weight1))

    val profilesWithIncome = joinedProfiles.joinWithSmaller('worktitle -> 'data, trained).project(('rowkey, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree, 'income))
            .rename('rowkey -> 'key)



    val combined = businessGroup.combineCheckinsProfiles(checkins, profilesWithIncome)


    val chkindata = checkinGroup.groupCheckins(TextLine(checkininput))
    val friendData = TextLine(friendinput).read.project('line)
    val profilesAndCheckins = combined.project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))
    val employerGroupedServiceProfiles = total.project(('key, 'worked))
    val serviceIds = total.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))
    val friendsForCoworker = friendGroup.groupFriends(friendData)
    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)
    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))
    val homeCheckins = checkinGroup.groupHomeCheckins(TextLine(checkininput).read)
    val homeProfilesAndCheckins = joinedProfiles.joinWithLarger('key -> 'keyid, homeCheckins).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))
    val findhomefromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(homeProfilesAndCheckins)
    val withHomeWork = combined.joinWithSmaller('key -> 'key1, findcityfromchkins)
            .map('centroid -> ('workCentroid)) {centroid: String => centroid}
            .discard(('key1, 'centroid))
            .joinWithSmaller('key -> 'key1, findhomefromchkins)
            .map('centroid -> ('homeCentroid)) {centroid: String => centroid}








    val byAge = businessGroup.byAge(combined)
            .map(('venueKey, 'ageBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, ageBracket, frequency) = in

            val targetVenueGoldenId = venueKey + "_age"
            val column = ageBracket
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))


    val byGender = businessGroup.byGender(combined)
            .map(('venueKey, 'impliedGender, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, impliedGender, frequency) = in

            val targetVenueGoldenId = venueKey + "_gender"
            val column = impliedGender
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))

    val byDegree = businessGroup.byDegree(combined)
            .map(('venueKey, 'degreeCat, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, degreeCat, frequency) = in

            val targetVenueGoldenId = venueKey + "_education"
            val column = degreeCat
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))

    val loyalty = reachLoyalty.findLoyalty(combined)

    val loyaltyCount = loyalty
            .map(('venueKey, 'loyalty, 'customers) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, customerType, frequency) = in

            val targetVenueGoldenId = venueKey + "_loyalty_customerCount"
            val column = customerType
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))

    val loyaltyVisits = loyalty
            .map(('venueKey, 'loyalty, 'visitsType) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, customerType, frequency) = in

            val targetVenueGoldenId = venueKey + "_loyalty_visitCount"
            val column = customerType
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))


    val reach = reachLoyalty.findReach(withHomeWork)

    val reachMean = reach
            .map(('venueKey, 'meanDist) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Double) =>
            val (venueKey, mean) = in

            val targetVenueGoldenId = venueKey + "_reach_distance"
            val column = "meanDist"
            val value = mean

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachStdev = reach
            .map(('venueKey, 'stdevDist) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Double) =>
            val (venueKey, stdev) = in

            val targetVenueGoldenId = venueKey + "_reach_distance"
            val column = "stdevDist"
            val value = stdev

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachHome = reach
            .map(('venueKey, 'numHome) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Int) =>
            val (venueKey, count) = in

            val targetVenueGoldenId = venueKey + "_reach_originCount"
            val column = "numHome"
            val value = count

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachWork = reach
            .map(('venueKey, 'numWork) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Int) =>
            val (venueKey, count) = in

            val targetVenueGoldenId = venueKey + "_reach_originCount"
            val column = "numWork"
            val value = count

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val byIncome = businessGroup.byIncome(profilesWithIncome)
            .map(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, income, frequency) = in

            val targetVenueGoldenId = venueKey + "_income"
            val column = income
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val staticOutput = byIncome.++(reachHome).++(reachWork).++(reachMean).++(reachStdev).++(loyaltyCount).++(loyaltyVisits).++(byAge).++(byDegree).++(byGender)
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueStatic",
            scheme = WideRowScheme(keyField = 'rowKey)
        )
    )

//    val byTime = businessGroup.timeSeries(combined)
//            .map(('venueKey, 'hourChunk, 'serType, 'size) ->('rowKey, 'columnName, 'columnValue)) {
//        in: (String, Int, String, Int) =>
//            val (venueKey, hour, serviceType, frequency) = in
//
//            val targetVenueGoldenId = venueKey + "_checkinFrequencyPerHour_" + serviceType
//            val column = hour.toLong * 3600000
//            val value = frequency * 1.0
//
//            (targetVenueGoldenId, column, value)
//
//    }
//            .project(('rowKey, 'columnName, 'columnValue))
//
//
//    val timeSeriesOutput = byTime
//            .write(
//        CassandraSource(
//            rpcHost = rpcHostArg,
//            privatePublicIpMap = ppmap,
//            keyspaceName = "dossier",
//            columnFamilyName = "MetricsVenueTimeseries",
//            scheme = WideRowScheme(keyField = 'rowKey)
//        )
//    )
}
