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
        }.groupAll {
            _.sortBy('buzzCount)
        }.project('stemmedVenName, 'buzzCount, 'goldenIdList)


    def findBuzzStats(buzz: RichPipe) =
        buzz.groupAll {
            _.average('buzzCount -> ('avg))
        }

    def findMin(buzz: RichPipe) =
        buzz.groupAll {
            _.min('normalized -> 'min)
        }.project('min)

    def findMax(buzz: RichPipe) =
        buzz.groupAll {
            _.max('normalized -> 'max)
        }.project('max)

    def normalizeBuzz(buzz: RichPipe, buzzStats: RichPipe) =
        buzz.crossWithTiny(buzzStats)
                .map(('buzzCount, 'avg) -> ('normalized)) {
            fields: (Double, String) =>
                val (buzz, avg) = fields
                val normalized = buzz / avg.toDouble
                math.log(normalized)
        }.project('stemmedVenName, 'buzzCount, 'normalized, 'goldenIdList)

    def calculateBuzzScore(normalizedBuzz: RichPipe, min: RichPipe, max: RichPipe): RichPipe = {
        val buzzScore = normalizedBuzz
                .crossWithTiny(min)
                .crossWithTiny(max)
                .map(('normalized, 'min, 'max) -> 'buzzScore) {
            fields: (Double, Double, Double) =>
                val (buzz, min, max) = fields
                var score = 0.0
                score = (buzz + (-min)) * (98 / (max + (-(min))))
                (score + 1.0)
        }
        buzzScore
    }
            .flatMap('goldenIdList -> 'golden) {
        fields: (List[String]) =>
            val (goldenIdList) = fields
            goldenIdList
    }
            .project('stemmedVenName, 'buzzCount, 'buzzScore, 'golden)


}
