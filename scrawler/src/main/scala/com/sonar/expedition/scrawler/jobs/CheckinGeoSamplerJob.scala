package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.util.{Haversine, CommonFunctions}
import CommonFunctions._
import collection.immutable.TreeMap
import Numeric.Implicits._
import Ordering.Implicits._
import ch.hsr.geohash.GeoHash

class CheckinGeoSamplerJob(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {

    val (newCheckins, allCheckinsWithGoldenId) = checkinSource(args, false, true)
    /*
    val selectedCheckins = allCheckinsWithGoldenId.filter('lat, 'lng) {
        in: (Double, Double) =>
        // NY 2 char
        // 7331860193359167488L
        // NY 4 char
            GeoHash.withCharacterPrecision(in._1, in._2, 4).longValue() == 7335079563405295616L
    }
    */
    val selectedCheckins = allCheckinsWithGoldenId.limit(1000)

    val income = SequenceFile(args("income"), ('worktitle, 'income, 'weight)).read

    val peopleCheckins = selectedCheckins
            .unique('goldenId, 'keyid, 'lat, 'lng)

    // loyalty
    val loyalty = selectedCheckins.groupBy('goldenId, 'keyid) {
        _.size('loyalty)
    }
    peopleCheckins.joinWithSmaller(('goldenId, 'keyid) ->('goldenId1, 'keyid1), loyalty.rename(('goldenId, 'keyid) ->('goldenId1, 'keyid1))).discard('goldenId1, 'keyid1)
            .joinWithSmaller('keyid -> 'key, serviceProfiles(args)).discard('key)
            .map('degree -> 'degreeCat) {
        degree: String =>
            degree match {
                case College(str) => "College"
                case NoCollege(str) => "NoCollege"
                case Grad(str) => "GradSchool"
                case _ => "unknown"
            }
    }.leftJoinWithSmaller('worktitle -> 'worktitle1, income.rename('worktitle -> 'worktitle1)).discard('worktitle1)
            .map(('impliedGender, 'degreeCat, 'income, 'age) -> 'features) {
        in: (String, String, String, Int) =>
            val (gender, degreeCat, incomeStr, age) = in
            // income parsing
            val income = if (incomeStr == null) -1
            else {
                val clean = incomeStr.replaceAll("\\D", "")
                if (clean.isEmpty) -1 else clean.toInt
            }

            val categoricalValues = Map("gender" -> gender, "education" -> degreeCat)
            val realValues = Map("age" -> age, "income" -> income)
            realValues ++ categoricalValues
        /* val result = realValues.map {
 case (feature, value) => feature + "=" + value
}.toSeq ++ powersetFeatures.toSeq.sortBy(_.length)
result.mkString(",")         */

    }
            // centroids
            .leftJoinWithSmaller('keyid -> 'key1, SequenceFile(args("centroids"), ('key1, 'workCentroid, 'homeCentroid))).discard('key1)
            // compute venue features
            .mapTo(('goldenId, 'lat, 'lng, 'workCentroid, 'homeCentroid, 'loyalty, 'features) -> 'features) {
        in: (String, Double, Double, String, String, Int, Map[String, Any]) =>
            val (goldenId, lat, lng, workCentroid, homeCentroid, loyalty, userFeatures) = in
            //distance calculation
            val workdist = if (workCentroid == null) -1
            else {
                val Array(otherLat, otherLng) = workCentroid.split(':')
                Haversine.haversineInMeters(lat, lng, otherLat.toDouble, otherLng.toDouble)
            }
            val homedist = if (homeCentroid == null) -1
            else {
                val Array(otherLat, otherLng) = homeCentroid.split(':')
                Haversine.haversineInMeters(lat, lng, otherLat.toDouble, otherLng.toDouble)
            }
            val minDistance = math.min(homedist, workdist)

            val realValues = Map[String, Any]("distance" -> minDistance, "loyalty" -> loyalty)
            import collection.JavaConversions._
            NewAggregateMetricsJob.ObjectMapper.writeValueAsString((userFeatures ++ realValues): java.util.Map[String, Any])
    }.write(TextLine(args("rawoutput")))

    def combine(sets: Iterable[Set[String]]) = sets.reduceLeft[Set[String]] {
        case (acc, set) =>
            for (a <- acc;
                 s <- set) yield {
                a + "_and_" + s
            }
    }

    def bucketedRealValues(features: Iterable[(String, Int)]) = for ((kind, value) <- features;
                                                                     granularity <- Seq("fine")) yield buckets(granularity, kind, value)

    def buckets(granularity: String, kind: String, value: Int) = {
        val name = granularity + "_" + kind
        val greaterEquals = FeatureExtractions.bucketMap(name).filter {
            case (low, _) => low <= value
        }
        if (greaterEquals.isEmpty) Set(kind + "_unknown")
        else {
            val (low, high) = greaterEquals.maxBy(_._1)
            val highName = if (high == Int.MaxValue) "+" else "-" + high
            greaterEquals.keySet.map(name + ">=" + _) + (name + "=" + low + highName)
        }
    }

}
