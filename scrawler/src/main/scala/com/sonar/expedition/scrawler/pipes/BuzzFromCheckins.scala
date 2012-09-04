package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.{StemAndMetaphoneEmployer, CommonFunctions, ShingleTokenizer}
import cascading.pipe.joiner.LeftJoin
import cascading.tuple.Fields
import com.sonar.dossier.dto.ServiceType


trait BuzzFromCheckins extends ScaldingImplicits {


    def getShingles(checkinsWithMessage: RichPipe) =
        checkinsWithMessage
                .flatMapTo('msg -> 'singleShingle) {
            message: String =>
                if (CommonFunctions.isNullOrEmpty(message))
                    List.empty[String]
                else ShingleTokenizer.shingleize(StemAndMetaphoneEmployer.removeStopWords(message), 3)
        }


    def findBuzz(shingles: RichPipe, checkinsWithNoMessages: RichPipe) =
        checkinsWithNoMessages
                .flatMap('venName -> 'stemmedVenName) {
            venName: String =>
                if (CommonFunctions.isNullOrEmpty(venName)) None
                else Some(StemAndMetaphoneEmployer.removeStopWords(venName))
        }.joinWithLarger('stemmedVenName -> 'singleShingle, shingles).groupBy('stemmedVenName, 'goldenId) {
            _.size('shinglesPerVenue)
        }.groupBy('stemmedVenName) {
            _.sum('shinglesPerVenue -> 'buzzCount).toList[String]('goldenId -> 'goldenIdList)
        }.project('stemmedVenName, 'buzzCount, 'goldenIdList)


    def findBuzzStats(buzz: RichPipe) =
        buzz.groupAll {
            _.average('buzzCount -> 'avg).min('buzzCount -> 'min).max('buzzCount -> 'max)
        }

    def normalize(buzz: Double, avg: Double) = math.log(buzz / avg)

    def calculateBuzzScore(normalizedBuzz: RichPipe, minMaxAvg: RichPipe) =
        normalizedBuzz
                .crossWithTiny(minMaxAvg)
                .flatMapTo(('goldenIdList, 'buzzCount, 'min, 'max, 'avg) ->('rowKey, 'columnName, 'columnValue)) {
            fields: (List[String], Double, Double, Double, Double) =>
                val (goldenIdList, buzzCount, min, max, avg) = fields
                val normalized = normalize(buzzCount, avg)
                val minNormalized = normalize(min, avg)
                val maxNormalized = normalize(max, avg)
                val buzzScore = (normalized - minNormalized) * (98 / (maxNormalized - minNormalized)) + 1.0
                goldenIdList flatMap {
                    goldenId => List((goldenId + "_normalizedBuzz", "buzzCount", buzzCount), (goldenId + "_normalizedBuzz", "buzzScore", buzzScore))
                }
        }


}
