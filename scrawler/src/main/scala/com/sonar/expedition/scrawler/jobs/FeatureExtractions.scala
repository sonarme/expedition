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

    //val (newCheckins, checkinsWithGoldenId) = checkinSource(args, false, true)
    val income = SequenceFile(args("income"), ('worktitle, 'income, 'weight)).read
    val profiles = serviceProfiles(args).map('degree -> 'degreeCat) {
        degree: String =>
            degree match {
                case College(str) => "College"
                case NoCollege(str) => "No College"
                case Grad(str) => "Grad School"
                case _ => "unknown"
            }
    }.leftJoinWithSmaller('worktitle -> 'worktitle1, income.rename('worktitle -> 'worktitle1))


    val userFeatures = profiles.map(('impliedGender, 'degreeCat, 'income, 'age) -> 'features) {
        in: (String, String, String, Int) =>
            val (gender, degreeCat, incomeStr, age) = in
            val income = if (incomeStr == null) -1 else incomeStr.replaceAll("\\D", "").toInt
            val categoricalValues = Set("gender_" + gender, "education_" + degreeCat)
            val realValues = Set("age" -> age.toDouble)
            val buckets = bucketing(realValues)
            val features = categoricalValues ++ buckets
            val powersetFeatures = powerset(features) map (_.mkString("_and_"))
            val result = realValues.map {
                case (feature, value) => feature + "=" + value
            }.toSeq ++ powersetFeatures.toSeq.sortBy(_.length)
            result.mkString(",")
    }
    userFeatures.write(Tsv("test", 'features))

    /*
  checkinsWithGoldenId
          .unique('goldenId, 'keyid, 'lat, 'lng)
          .joinWithSmaller('keyid -> 'key, userFeatures)
          .leftJoinWithSmaller('key -> 'key1, SequenceFile(args("centroids"), ('key1, 'workCentroid, 'homeCentroid)))
          .map(('lat, 'lng, 'workCentroid, 'homeCentroid) ->('workDistance, 'homeDistance, 'minDistance)) {
      in: (Double, Double, String, String) =>
          val (lat, lng, workCentroid, homeCentroid) = in
          val workdist = if (workCentroid==null) -1 else {
              val Array(otherLat, otherLng) = workCentroid.split(':')
              Haversine.haversineInMeters(lat, lng, otherLat.toDouble, otherLng.toDouble)
          }
          val homedist = if (homeCentroid==null) -1 else {
              val Array(otherLat, otherLng) = homeCentroid.split(':')
              Haversine.haversineInMeters(lat, lng, otherLat.toDouble, otherLng.toDouble)
          }

          (workdist, homedist, math.min(homedist, workdist))
  }
          .groupBy('goldenId) {
      _.foldLeft('features -> 'featuresCount)(Map.empty[String, Int]) {
          (agg: Map[String, Int], features: Set[String]) => agg ++ features.map(feature => feature -> (agg.getOrElse(feature, 0) + 1))
      }
  }.write(Tsv("testFeatures", ('goldenId, 'featuresCount)))*/


    def powerset[A](s: Set[A]) = s.foldLeft(Set(Set.empty[A])) {
        case (ss, el) => ss ++ ss.map(_ + el)
    } - Set.empty[A]

    def bucketing(features: Iterable[(String, Double)]) = for ((kind, value) <- features;
                                                               granularity <- Seq("coarse", "fine");
                                                               bucket <- buckets(granularity, kind, value)) yield bucket

    def buckets(granularity: String, kind: String, value: Double) = {
        val name = granularity + "_" + kind
        val greaterEquals = FeatureExtractions.bucketMap(name).filter {
            case (low, _) => low <= value
        }
        if (greaterEquals.isEmpty) Seq(kind + "_unknown")
        else {
            val (low, high) = greaterEquals.maxBy(_._1)
            (name + "=" + low + "-" + high) :: greaterEquals.map(name + ">=" + _._1).toList
        }
    }

}

object FeatureExtractions {
    val bucketMap = Map(
        "coarse_age" -> Map(0 -> 13, 13 -> 25, 25 -> 50, 50 -> 100),
        "fine_age" -> Map(0 -> 6, 6 -> 13, 13 -> 19, 19 -> 25, 25 -> 37, 37 -> 50, 50 -> 75, 75 -> 100)

    )
}
