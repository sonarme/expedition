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

class FeatureExtractions(args: Args) extends DefaultJob(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {

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
    val selectedCheckins = SequenceFile(args("dealsOutput"), DealAnalysis.DealsOutputTuple).read.unique('dealId, 'goldenId).joinWithLarger('goldenId -> 'goldenId1, allCheckinsWithGoldenId.rename('goldenId -> 'goldenId1)).discard('goldenId1)

    val income = SequenceFile(args("income"), ('worktitle, 'income, 'weight)).read

    val numCheckins = selectedCheckins.groupBy('goldenId) {
        _.size('numCheckins)
    }

    val peopleCheckins = selectedCheckins
            .unique('dealId, 'goldenId, 'keyid, 'lat, 'lng)
    val numPeople = peopleCheckins.groupBy('goldenId) {
        _.size('numPeople)
    }
    numCheckins.leftJoinWithSmaller('goldenId -> 'goldenId1, numPeople.rename('goldenId -> 'goldenId1)).discard('goldenId1).write(Tsv(args("numOutput"), ('goldenId, 'numCheckins, 'numPeople)))

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
            val income = parseIncome(incomeStr)

            val categoricalValues = Set("gender_" + gender, "education_" + degreeCat)
            val realValues = Set("age" -> age, "income" -> income)
            val buckets = FeatureExtractions.bucketedRealValues(realValues)
            val features = categoricalValues.map(x => Set(x)) ++ buckets
            features

    }
            // centroids
            .leftJoinWithSmaller('keyid -> 'key1, SequenceFile(args("centroids"), ('key1, 'workCentroid, 'homeCentroid))).discard('key1)
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
            val buckets = FeatureExtractions.bucketedRealValues(realValues)
            val raw = (userFeatures ++ buckets).flatten.toSet[String]
            val powersetFeatures = FeatureExtractions.combine(userFeatures ++ buckets)
            raw ++ powersetFeatures
    }.write(SequenceFile(args("rawoutput"), FeatureExtractions.RawTuple))
            .write(Tsv(args("rawoutput") + "_tsv", FeatureExtractions.RawTuple))


}

object FeatureExtractions extends TupleConversions {
    val RawTuple = ('dealId, 'goldenId, 'keyid, 'features)
    val OutputTuple = ('dealId, 'goldenId, 'featuresCount, 'numCheckins, 'numPeople)
    val bucketMap = Map(
        //  "coarse_age" -> Map(0 -> 13, 13 -> 25, 25 -> 50, 50 -> 100),
        "age" -> Set(
            0 -> 13,
            8 -> 16,
            13 -> 19,
            16 -> 22,
            19 -> 25,
            22 -> 28,
            25 -> 31,
            28 -> 34,
            31 -> 37,
            34 -> 41,
            37 -> 50,
            41 -> 60,
            50 -> Int.MaxValue),
        "distance" -> Set(
            0 -> 1000,
            320 -> 1500,
            1000 -> 2000,
            1500 -> 3000,
            2000 -> 4000,
            3000 -> 6000,
            4000 -> 8000,
            6000 -> 10000,
            8000 -> 12000,
            12000 -> Int.MaxValue),
        "loyalty" -> Set(
            1 -> 2,
            1 -> 3,
            2 -> 3,
            2 -> 4,
            3 -> 4,
            3 -> 6,
            4 -> 10,
            6 -> 12,
            10 -> Int.MaxValue),
        "income" -> Set(
            0 -> 30000,
            15000 -> 40000,
            30000 -> 50000,
            40000 -> 62000,
            50000 -> 75000,
            62000 -> 87000,
            75000 -> 100000,
            87000 -> 125000,
            100000 -> 150000,
            125000 -> 200000,
            150000 -> Int.MaxValue)
    )

    def combine(sets: Iterable[Set[String]]) = sets.toSeq.combinations(2).flatMap {
        case Seq(a, b) => for (a1 <- a; b1 <- b) yield a1 + "_and_" + b1
        case _ => Iterable.empty
    }

    /*sets.reduceLeft[Set[String]] {
        case (acc, set) =>
            for (a <- acc;
                 s <- set) yield {
                a + "_and_" + s
            }
    }*/


    def bucketedRealValues(features: Iterable[(String, Int)]) = for ((kind, value) <- features;
                                                                     granularity <- Seq("fine")) yield buckets(granularity, kind, value)

    def buckets(granularity: String, kind: String, value: Int) = {
        val name = /*granularity + "_" + */ kind
        /*  val greaterEquals = FeatureExtractions.bucketMap(name).filter {
            case (low, _) => low <= value
        }*/
        val matchBuckets = bucketMap(name).filter {
            case (low, high) =>
                low <= value && value < high
        }
        if ( /*greaterEquals.isEmpty ||*/ matchBuckets.isEmpty) Set(kind + "_unknown")
        else {
            (matchBuckets map {
                case (low, high) =>
                    val highName = if (high == Int.MaxValue) "+" else "-" + high
                    name + "=" + low + highName
            }).toSet[String]
            /*val (low, high) = matchBuckets.maxBy(_._1)
            val highName = if (high == Int.MaxValue) "+" else "-" + high
            greaterEquals.keySet.map(name + ">=" + _) + (name + "=" + low + highName)*/
        }
    }
}
