package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{DateSerializer, LongSerializer, StringSerializer, DoubleSerializer}
import cascading.tuple.Fields
import java.nio.ByteBuffer
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.twitter.scalding.SequenceFile
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.TextLine
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.expedition.scrawler.util.CommonFunctions

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTap(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {


    val input = args("serviceProfileInput")
    val twinput = args("twitterServiceProfileInput")
    //    val checkininput = args("checkinInput")
    val friendinput = args("friendInput")
    val bayesmodel = args("bayesmodelforsalary")
    //    val newcheckininput = args("newCheckinInput")
    val sequenceOutputStatic = args("sequenceOutputStatic")
    val sequenceOutputTime = args("sequenceOutputTime")
    val textOutputStatic = args("textOutputStatic")
    val textOutputTime = args("textOutputTime")
    val timeSeriesOnly = args.getOrElse("timeSeriesOnly", "false").toBoolean

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


    val (newCheckins, checkinsWithGoldenId) = checkinSource(args, false, true)

    //    val checkins = unfilteredCheckinsLatLon(TextLine(checkininput))
    //    val newcheckins = correlationCheckins(TextLine(newcheckininput))

    val checkinsWithGoldenIdAndLoc = checkinsWithGoldenId
            .map(('lat, 'lng) -> 'loc) {
        fields: (String, String) =>
            val (lat, lng) = fields
            lat + ":" + lng
    }

    val total = getTotalProfileTuples(data, twdata).map('uname ->('impliedGender, 'impliedGenderProb)) {
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

    if (!timeSeriesOnly) {

        val chkindata = groupCheckins(newCheckins)

        val friendData = TextLine(friendinput).read.project('line)

        val profilesAndCheckins = combined.project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

        val employerGroupedServiceProfiles = total.project(('key, 'worked))

        val serviceIds = total.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

        val friendsForCoworker = groupFriends(friendData)

        val coworkerCheckins = findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata).write(TextLine("/tmp/coworkerCheckins"))

        val findcityfromchkins = findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))

        val homeCheckins = groupHomeCheckins(newCheckins)

        val homeProfilesAndCheckins = profiles.joinWithLarger('key -> 'keyid, homeCheckins).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

        val findhomefromchkins = findClusteroidofUserFromChkins(homeProfilesAndCheckins)

        val withHomeWork = combined.joinWithSmaller('key -> 'key1, findcityfromchkins)
                .map('centroid -> 'workCentroid) {
            centroid: String => centroid
        }
                .discard(('key1, 'centroid))
                .joinWithSmaller('key -> 'key1, findhomefromchkins)
                .map('centroid -> 'homeCentroid) {
            centroid: String => centroid
        }
        //            .map('centroid -> 'homeCentroid) {centroid: String => "0.0:0.0"}

        val byAge = groupByAge(combined)
                .map(('venueKey, 'ageBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, ageBracket, frequency) = in

                val targetVenueGoldenId = venueKey + "_age"
                val column = ageBracket
                val value = frequency.toDouble

                (targetVenueGoldenId, column, value)

        }
                .project(('rowKey, 'columnName, 'columnValue))


        val byGender = groupByGender(combined)
                .map(('venueKey, 'impliedGender, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, Gender, Int) =>
                val (venueKey, impliedGender, frequency) = in

                val targetVenueGoldenId = venueKey + "_gender"
                val column = impliedGender
                val value = frequency.toDouble

                (targetVenueGoldenId, column, value)

        }
                .project(('rowKey, 'columnName, 'columnValue))

        val byDegree = groupByDegree(combined)
                .map(('venueKey, 'degreeCat, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, degreeCat, frequency) = in

                val targetVenueGoldenId = venueKey + "_education"
                val column = degreeCat
                val value = frequency.toDouble

                (targetVenueGoldenId, column, value)

        }
                .project(('rowKey, 'columnName, 'columnValue))

        val totalDegree = byDegree.groupBy('columnName) {
            _.sum('columnValue)
        }
                .map('columnName -> 'rowKey) {
            columnName: String => "totalAll_education"
        }

        val totalAge = byAge.groupBy('columnName) {
            _.sum('columnValue)
        }
                .map('columnName -> 'rowKey) {
            columnName: String => "totalAll_age"
        }

        val totalGender = byGender.groupBy('columnName) {
            _.sum('columnValue)
        }
                .map('columnName -> 'rowKey) {
            columnName: String => "totalAll_gender"
        }

        /* val totalIncome = byIncome.groupBy('columnName) {
           _.sum('columnValue)
       }
               .map('columnName -> 'rowKey) {
           columnName: String => "totalAll_income"
       } */

        val totalStatic = (totalAge ++ totalDegree ++ totalGender).project(('rowKey, 'columnName, 'columnValue))


        val loyalty = findLoyalty(combined)

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


        val reach = findReach(withHomeWork)

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

        val reachLat = reach
                .map(('venueKey, 'lat) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, Double) =>
                val (venueKey, lat) = in

                val targetVenueGoldenId = venueKey + "_reach_distance"
                val column = "latitude"
                val value = lat

                (targetVenueGoldenId, column, value)
        }
                .project(('rowKey, 'columnName, 'columnValue))

        val reachLong = reach
                .map(('venueKey, 'lng) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, Double) =>
                val (venueKey, lng) = in

                val targetVenueGoldenId = venueKey + "_reach_distance"
                val column = "longitude"
                val value = lng

                (targetVenueGoldenId, column, value)
        }
                .project(('rowKey, 'columnName, 'columnValue))

        val reachHome = reach
                .map(('venueKey, 'numHome) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, Int) =>
                val (venueKey, count) = in

                val targetVenueGoldenId = venueKey + "_reach_originCount"
                val column = "numHome"
                val value = count.toDouble

                (targetVenueGoldenId, column, value)
        }
                .project(('rowKey, 'columnName, 'columnValue))

        val reachWork = reach
                .map(('venueKey, 'numWork) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, Int) =>
                val (venueKey, count) = in

                val targetVenueGoldenId = venueKey + "_reach_originCount"
                val column = "numWork"
                val value = count.toDouble

                (targetVenueGoldenId, column, value)
        }
                .project(('rowKey, 'columnName, 'columnValue))

        /* val byIncome = byIncome(combined)
   .map(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
in: (String, String, Int) =>
   val (venueKey, income, frequency) = in

   val targetVenueGoldenId = venueKey + "_income"
   val column = income
   val value = frequency.toDouble

   (targetVenueGoldenId, column, value)
}
   .project(('rowKey, 'columnName, 'columnValue))   */

        val staticOutput =
            (totalStatic ++ reachHome ++ reachWork ++ reachLat ++ reachLong ++ reachMean ++ reachStdev ++ loyaltyCount ++ loyaltyVisits ++ byAge ++ byDegree ++ byGender)

        val staticSequence = staticOutput.write(SequenceFile(sequenceOutputStatic, Fields.ALL))
        val staticText = staticOutput.write(TextLine(textOutputStatic))
    }

    val byTime = timeSeries(combined)
            .map(('venueKey, 'hourChunk, 'serType, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Int, String, Int) =>
            val (venueKey, hour, serviceType, frequency) = in

            val targetVenueGoldenId = venueKey + "_checkinFrequencyPerHour_" + serviceType
            val column = hour.toLong * 3600000
            val value = frequency * 1.0

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))

    val timeSequence = byTime.write(SequenceFile(sequenceOutputTime, Fields.ALL))
    val timeText = byTime.write(TextLine(textOutputTime))


}
