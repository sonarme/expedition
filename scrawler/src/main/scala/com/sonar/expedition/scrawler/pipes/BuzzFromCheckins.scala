package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.{StemAndMetaphoneEmployer, CommonFunctions, ShingleTokenizer}
import cascading.pipe.joiner.LeftJoin
import cascading.tuple.Fields
import com.sonar.dossier.dto.ServiceType
import collection.JavaConversions._


trait BuzzFromCheckins extends ScaldingImplicits {


    def getShingles(checkinsWithMessage: RichPipe) =
        checkinsWithMessage
                .flatMapTo('msg -> 'singleShingle) {
            message: String =>
                if (CommonFunctions.isNullOrEmpty(message))
                    List.empty[String]
                else ShingleTokenizer.shingleize(StemAndMetaphoneEmployer.removeStopWords(message), 3)
        }


    def findBuzz(shingles: RichPipe, placeClassification: RichPipe) =
        placeClassification.joinWithLarger('stemmedVenName -> 'singleShingle, shingles).groupBy('stemmedVenName) {
            _.size('buzzCount)
        }


    def findBuzzStats(buzz: RichPipe) =
        buzz.groupAll {
            _.average('buzzCount -> 'avg).min('buzzCount -> 'min).max('buzzCount -> 'max)
        }

    def normalize(buzz: Double, avg: Double) = math.log(buzz / avg)

    def calculateBuzzScore(normalizedBuzz: RichPipe, minMaxAvg: RichPipe) =
        normalizedBuzz
                .crossWithTiny(minMaxAvg)
                .map(('buzzCount, 'min, 'max, 'avg) -> 'buzzScore) {
            fields: (Double, Double, Double, Double) =>
                val (buzzCount, min, max, avg) = fields
                val normalized = normalize(buzzCount, avg)
                val minNormalized = normalize(min, avg)
                val maxNormalized = normalize(max, avg)
                val buzzScore = (normalized - minNormalized) * (98 / (maxNormalized - minNormalized)) + 1.0
                buzzScore
        }


}
