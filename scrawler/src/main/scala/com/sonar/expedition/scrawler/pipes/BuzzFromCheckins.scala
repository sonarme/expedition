package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.{StemAndMetaphoneEmployer, CommonFunctions, ShingleTokenizer}
import cascading.pipe.joiner.LeftJoin
import cascading.tuple.Fields
import com.sonar.dossier.dto.ServiceType
import JobImplicits._

trait BuzzFromCheckins extends ScaldingImplicits {


    def getShingles(checkinsWithMessage: RichPipe): RichPipe = {
        val messagePipe = checkinsWithMessage

        val shinglePipe = messagePipe
                .filter('msg) {
            fields: (String) =>
                val message = fields
                (!CommonFunctions.isNullOrEmpty(message))
        }
                .map('msg -> 'message) {
            fields: (String) =>
                val message = fields
                val msg = StemAndMetaphoneEmployer.getStemmed(message)
                msg
        }
                .flatMap(('venName, 'message) -> 'singleShingle) {
            fields: (String, String) =>
                val (venueName, message) = fields
                val messageShingles = ShingleTokenizer.shingleize(message, 3)
                (messageShingles)
        }
                .project('singleShingle)
        shinglePipe
    }

    def findBuzz(shingles: RichPipe, checkinsWithNoMessages: RichPipe): RichPipe = {
        val buzz = checkinsWithNoMessages
                .map('venName -> 'stemmedVenName) {
            fields: (String) =>
                val (venName) = fields
                val stemmedVenName = StemAndMetaphoneEmployer.getStemmed(venName)
                (stemmedVenName)
        }
                .filter('stemmedVenName) {
            fields: (String) =>
                val venName = fields
                (!CommonFunctions.isNullOrEmpty(venName))
        }
                .joinWithLarger('stemmedVenName -> 'singleShingle, shingles)
                //                .unique(('stemmedVenName, 'goldenId))
                .groupBy('stemmedVenName, 'goldenId) {
            _.size('shinglesPerVenue)
        }
                .groupBy('stemmedVenName) {
            _
                    .sum('shinglesPerVenue -> 'buzzCount)
                    .toList[String]('goldenId -> 'goldenIdList)
            //                    .sortWithTake('goldenId -> 'goldenIdList, 100000) {
            //                (venueId1: (String), venueId2: (String)) => venueId1 > venueId2
            //            }
        }
                .groupAll {
            _
                    .sortBy('buzzCount)
        }
                .project('stemmedVenName, 'buzzCount, 'goldenIdList)
        buzz
    }

    def findBuzzStats(buzz: RichPipe): RichPipe = {
        val buzzStats = buzz
                //                .filter('buzzCount) {
                //            fields: (String) =>
                //                val (size) = fields
                //                (size.toInt > 1)
                //        }
                .groupAll {
            _
                    .average('buzzCount -> ('avg))
        }
        buzzStats
    }

    def findMin(buzz: RichPipe): RichPipe = {
        val min = buzz
                .groupAll {
            _
                    .min('normalized -> 'min)
        }.project('min)
        min
    }

    def findMax(buzz: RichPipe): RichPipe = {
        val max = buzz
                .groupAll {
            _
                    .max('normalized -> 'max)
        }.project('max)
        max
    }

    def normalizeBuzz(buzz: RichPipe, buzzStats: RichPipe): RichPipe = {
        val normalizedBuzz = buzz
                .crossWithTiny(buzzStats)
                .map(('buzzCount, 'avg) -> ('normalized)) {
            fields: (Double, String) =>
                val (buzz, avg) = fields
                val normalized = buzz / avg.toDouble
                val log = scala.math.log(normalized)
                log
        }
                .project('stemmedVenName, 'buzzCount, 'normalized, 'goldenIdList)
        normalizedBuzz

    }

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
