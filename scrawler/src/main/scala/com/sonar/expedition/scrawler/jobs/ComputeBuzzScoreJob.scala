package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.util.{StemAndMetaphoneEmployer, CommonFunctions}

class ComputeBuzzScoreJob(args: Args) extends Job(args) with CheckinSource with CheckinGrouperFunction with BuzzFromCheckins with PlacesCorrelation {

    val output = args("output")
    val (checkins, _) = checkinSource(args, false, false)
    val shinglesPipe = getShingles(checkins)
    val placeClassification = SequenceFile(args("placeClassification"), PlaceClassification.PlaceClassificationOutputTuple)
            .flatMap('venName -> 'stemmedVenName) {
        venName: String =>
            if (CommonFunctions.isNullOrEmpty(venName)) None
            else Some(StemAndMetaphoneEmployer.removeStopWords(venName))
    }
    val buzz = findBuzz(shinglesPipe, placeClassification)
    val buzzStats = findBuzzStats(buzz)
    val scores = calculateBuzzScore(buzz, buzzStats)
    scores.joinWithLarger('stemmedVenName -> 'stemmedVenName, placeClassification).flatMapTo(
        ('goldenId, 'buzzCount, 'buzzScore) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Double, Double) =>
            val (goldenId, buzzCount, buzzScore) = in
            List((goldenId + "_normalizedBuzz", "buzzCount", buzzCount), (goldenId + "_normalizedBuzz", "buzzScore", buzzScore))
    }
            .write(SequenceFile(output, ('rowKey, 'columnName, 'columnValue)))
            .write(Tsv(output + "_tsv", ('rowKey, 'columnName, 'columnValue)))
}
