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

class FeatureExtractions(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {

    val (newCheckins, checkinsWithGoldenId) = checkinSource(args, false, true)
    val income = SequenceFile(args("income"), ('worktitle, 'income, 'weight)).read
    val profiles = serviceProfiles(args).map('degree -> 'degreeCat) {
        degree: String =>
            degree match {
                case College(str) => "College"
                case NoCollege(str) => "NoCollege"
                case Grad(str) => "GradSchool"
                case _ => "unknown"
            }
    }.leftJoinWithSmaller('worktitle -> 'worktitle1, income.rename('worktitle -> 'worktitle1))


    val userFeatures = profiles.map(('impliedGender, 'degreeCat, 'income, 'age) -> 'features) {
        in: (String, String, String, Int) =>
            val (gender, degreeCat, incomeStr, age) = in

            val income = if (incomeStr == null) -1
            else {
                val clean = incomeStr.replaceAll("\\D", "")
                if (clean.isEmpty) -1 else clean.toInt
            }
            val categoricalValues = Set("gender_" + gender, "education_" + degreeCat)
            val realValues = Set("age" -> age, "income" -> income)
            val buckets = bucketedRealValues(realValues)
            val features = categoricalValues.map(x => Set(x)) ++ buckets
            features
        /* val result = realValues.map {
   case (feature, value) => feature + "=" + value
}.toSeq ++ powersetFeatures.toSeq.sortBy(_.length)
result.mkString(",")         */

    } //.write(Tsv("testout",Fields.ALL))

    val loyalty = checkinsWithGoldenId.groupBy('goldenId, 'keyid) {
        _.size('loyalty)
    }

    checkinsWithGoldenId
            .unique('goldenId, 'keyid, 'lat, 'lng)
            // loyalty
            .joinWithSmaller(('goldenId, 'keyid) ->('goldenId1, 'keyid1), loyalty.rename(('goldenId, 'keyid) ->('goldenId1, 'keyid1)))
            // per-user features
            .joinWithSmaller('keyid -> 'key, userFeatures)
            // centroids
            .leftJoinWithSmaller('key -> 'key1, SequenceFile(args("centroids"), ('key1, 'workCentroid, 'homeCentroid)))
            // compute venue features
            .map(('lat, 'lng, 'workCentroid, 'homeCentroid, 'loyalty, 'features) -> 'features) {
        in: (Double, Double, String, String, Int, Iterable[Set[String]]) =>
            val (lat, lng, workCentroid, homeCentroid, loyalty, userFeatures) = in
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

            val realValues = Set("distance" -> minDistance, "loyalty" -> loyalty)
            val buckets = bucketedRealValues(realValues)
            val powersetFeatures = combine(userFeatures ++ buckets)
            powersetFeatures
    }
            .groupBy('goldenId) {
        _.foldLeft('features -> 'featuresCount)(Map.empty[String, Int]) {
            (agg: Map[String, Int], features: Set[String]) => agg ++ features.map(feature => feature -> (agg.getOrElse(feature, 0) + 1))
        }
    }.write(Tsv(args("output"), ('goldenId, 'featuresCount)))

    def combine(sets: Iterable[Set[String]]) = sets.reduceLeft[Set[String]] {
        case (acc, set) =>
            for (a <- acc; s <- set) yield {
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

object FeatureExtractions {
    val bucketMap = Map(
        //  "coarse_age" -> Map(0 -> 13, 13 -> 25, 25 -> 50, 50 -> 100),
        "fine_age" -> Map(1 -> 6, 6 -> 13, 13 -> 19, 19 -> 25, 25 -> 37, 37 -> 50, 50 -> 75, 75 -> Int.MaxValue),
        "fine_distance" -> Map(0 -> 1000, 1000 -> 2000, 2000 -> 5000, 5000 -> 10000, 10000 -> Int.MaxValue),
        "fine_loyalty" -> Map(1 -> 2, 2 -> 4, 4 -> 10, 10 -> Int.MaxValue),
        "fine_income" -> Map(1 -> 50000, 50000 -> 100000, 100000 -> 150000, 150000 -> 250000, 250000 -> 500000, 500000 -> Int.MaxValue)
    )
}
