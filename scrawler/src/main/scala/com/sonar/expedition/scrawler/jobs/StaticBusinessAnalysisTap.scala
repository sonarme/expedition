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
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.expedition.scrawler.util.CommonFunctions

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTap(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {


    val sequenceOutputStaticOption = args.optional("staticOutput")
    val sequenceOutputTimeOption = args.optional("timeOutput")

    val (newCheckins, checkinsWithGoldenId) = checkinSource(args, false, true)

    val checkinsWithGoldenIdAndLoc = checkinsWithGoldenId
            .map(('lat, 'lng) -> 'loc) {
        fields: (String, String) =>
            val (lat, lng) = fields
            lat + ":" + lng
    }.map('goldenId -> 'venueKey) {
        goldenId: String => goldenId
    }
    val profiles = serviceProfiles(args)


    val income = SequenceFile(args("income"), ('worktitle, 'income, 'weight)).read
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


    val combined = checkinsWithGoldenIdAndLoc.joinWithSmaller('keyid -> 'key, profiles).discard('key)

    sequenceOutputStaticOption foreach {
        sequenceOutputStatic =>

            val withHomeWork = combined.joinWithSmaller('keyid -> 'key1, SequenceFile(args("centroids"), ('key1, 'workCentroid, 'homeCentroid))).discard('key1)


            val ageGenderDegreeCheckins = ageGenderDegree(combined)

            /* val totalIncome = byIncome.groupBy('columnName) {
               _.sum('columnValue)
           }
                   .map('columnName -> 'rowKey) {
               columnName: String => "totalAll_income"
           } */


            val totalCheckins = checkinsWithGoldenId.groupBy('goldenId) {
                _.size
            }.mapTo(('goldenId, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, size) = in
                    (venueKey + "_numCheckins", "all", size.toDouble)

            } ++ combined.groupBy('venueKey) {
                _.size
            }.mapTo(('venueKey, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, size) = in
                    (venueKey + "_numCheckins", "withProfile", size.toDouble)

            }



            val visits = checkinsWithGoldenId.rename('goldenId -> 'venueKey).groupBy('keyid, 'venueKey) {
                _.size('visits)
            }

            val visitsStats = visits.groupBy('venueKey) {
                _.sizeAveStdev('visits ->('_size_, 'visitsAve, 'visitsStdev))
            }.discard('_size_).flatMapTo(('venueKey, 'visitsAve, 'visitsStdev) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double, Double) =>
                    val (venueKey, visitsAve, visitsStdev) = in
                    List((venueKey + "_visits", "ave", visitsAve), (venueKey + "_visits", "stdev", visitsStdev))
            }

            val loyalty = visits.map('visits -> 'loyalty) {
                size: Int =>
                    if (size <= 1)
                        "Passers-By"
                    else if (size <= 3)
                        "Regulars"
                    else
                        "Addicts"

            }.groupBy('venueKey, 'loyalty) {
                _.size('customers).sum('visits -> 'loyaltyVisitCount)
            }.flatMapTo(('venueKey, 'loyalty, 'customers, 'loyaltyVisitCount) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, String, Int, Int) =>
                    val (venueKey, customerType, customers, loyaltyVisitCount) = in
                    List((venueKey + "_loyalty_customerCount", customerType, customers.toDouble), (venueKey + "_loyalty_visitCount", customerType, loyaltyVisitCount.toDouble))
            }



            val reach = findReach(withHomeWork)

            val reachMean = reach
                    .mapTo(('venueKey, 'meanDist) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double) =>
                    val (venueKey, mean) = in
                    (venueKey + "_reach_distance", "meanDist", mean)
            }

            val reachStdev = reach
                    .mapTo(('venueKey, 'stdevDist) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double) =>
                    val (venueKey, stdev) = in
                    (venueKey + "_reach_distance", "stdevDist", stdev)
            }

            val latLng = checkinsWithGoldenIdAndLoc.groupBy('venueKey) {
                _.head('lat, 'lng)
            }
            val reachLat = latLng
                    .mapTo(('venueKey, 'lat) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double) =>
                    val (venueKey, lat) = in
                    (venueKey + "_reach_distance", "latitude", lat)
            }

            val reachLong = latLng
                    .mapTo(('venueKey, 'lng) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double) =>
                    val (venueKey, lng) = in
                    (venueKey + "_reach_distance", "longitude", lng)
            }


            val reachHome = reach
                    .mapTo(('venueKey, 'numHome) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, count) = in
                    (venueKey + "_reach_originCount", "numHome", count.toDouble)
            }

            val reachWork = reach
                    .mapTo(('venueKey, 'numWork) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, count) = in
                    (venueKey + "_reach_originCount", "numWork", count.toDouble)
            }

            val income = SequenceFile(args("income"), ('worktitle, 'income, 'weight)).read
            val byIncome = groupByIncome(combined.joinWithSmaller('worktitle -> 'worktitle1, income.rename('worktitle -> 'worktitle1)).discard('worktitle1))
                    .map(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, String, Int) =>
                    val (venueKey, income, frequency) = in
                    (venueKey + "_income", income, frequency.toDouble)
            }.project(('rowKey, 'columnName, 'columnValue))

            val staticOutput =
                (byIncome ++ ageGenderDegreeCheckins ++ totalCheckins ++ reachHome ++ reachWork ++ reachLat ++ reachLong ++ reachMean ++ reachStdev ++ loyalty ++ visitsStats)

            staticOutput.write(SequenceFile(sequenceOutputStatic, Fields.ALL))
                    .write(Tsv(sequenceOutputStatic + "_tsv", Fields.ALL))
    }
    sequenceOutputTimeOption foreach {
        sequenceOutputTime =>
            val byTime = timeSeries(chunkTime(combined))
                    .mapTo(('venueKey, 'hourChunk, 'serType, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int, String, Int) =>
                    val (venueKey, hour, serviceType, frequency) = in
                    (venueKey + "_checkinFrequencyPerHour_" + serviceType, hour.toLong * 3600000, frequency.toDouble)

            }

            byTime.write(SequenceFile(sequenceOutputTime, Fields.ALL))
                    .write(Tsv(sequenceOutputTime + "_tsv", Fields.ALL))
    }


    def ageGenderDegree(combined: RichPipe, postfix: String = "") = {


        val ageMetrics = combined.groupBy('venueKey) {
            _.sizeAveStdev('age ->('ageSize, 'ageAve, 'ageStdev))
        }.flatMapTo(('venueKey, 'ageSize, 'ageAve, 'ageStdev) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, Int, Double, Double) =>
                val (venueKey, ageSize, ageAve, ageStdev) = in
                List("ageSize" -> ageSize, "ageAve" -> ageAve, "ageStdev" -> ageStdev) map {
                    case (metricName, metricValue) => (venueKey + "_" + metricName + postfix, "metric", metricValue)
                }
        }

        val byAge = groupByAge(combined)
                .mapTo(('venueKey, 'ageBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, ageBracket, frequency) = in
                (venueKey + "_age" + postfix, ageBracket, frequency.toDouble)
        }

        val byGender = groupByGender(combined)
                .mapTo(('venueKey, 'impliedGender, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, impliedGender, frequency) = in
                (venueKey + "_gender" + postfix, impliedGender, frequency.toDouble)

        }

        val byDegree = groupByDegree(combined)
                .mapTo(('venueKey, 'degreeCat, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, degreeCat, frequency) = in
                (venueKey + "_education" + postfix, degreeCat, frequency.toDouble)

        }
        val totalDegree = byDegree.groupBy('columnName) {
            _.sum('columnValue)
        }.map(() -> 'rowKey) {
            u: Unit => "totalAll_education" + "_" + postfix
        }

        val totalAge = byAge.groupBy('columnName) {
            _.sum('columnValue)
        }.map(() -> 'rowKey) {
            u: Unit => "totalAll_age" + "_" + postfix
        }

        val totalGender = byGender.groupBy('columnName) {
            _.sum('columnValue)
        }.map(() -> 'rowKey) {
            u: Unit => "totalAll_gender" + "_" + postfix
        }
        val totalStatic = (totalAge ++ totalDegree ++ totalGender).project('rowKey, 'columnName, 'columnValue)
        byAge ++ byDegree ++ byGender ++ ageMetrics ++ totalStatic
    }
}
