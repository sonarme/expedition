package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Job, Args, RichPipe}
import com.sonar.expedition.scrawler.util.{StemAndMetaphoneEmployer, CommonFunctions, ShingleTokenizer}
import cascading.pipe.joiner.LeftJoin
import cascading.tuple.Fields

class BuzzFromCheckins(args: Args) extends Job(args) {


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
                val messageShingles = ShingleTokenizer.shingleize(message, venueName.split(" ").size + 1)
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
                .joinWithSmaller('stemmedVenName -> 'singleShingle, shingles, joiner = new LeftJoin)
                .groupBy('stemmedVenName) {
            _
                    .size('singleShinglesize)
        }
                .groupAll {
            _
                    .sortBy('singleShinglesize)
        }
                .project('stemmedVenName, 'singleShinglesize)
        buzz
    }

    def findBuzzStats(buzz: RichPipe): RichPipe = {
        val buzzStats = buzz
                //                .filter('singleShinglesize) {
                //            fields: (String) =>
                //                val (size) = fields
                //                (size.toInt > 1)
                //        }
                .groupAll {
            _
                    .sizeAveStdev('singleShinglesize ->('size, 'avg, 'stdev))
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

    def normalizedBuzz(buzz: RichPipe, buzzStats: RichPipe): RichPipe = {
        val normalizedBuzz = buzz
                .crossWithTiny(buzzStats)
                .map(('singleShinglesize, 'avg) -> ('normalized)) {
            fields: (String, String) =>
                val (buzz, avg) = fields
                val normalized = buzz.toInt / avg.toDouble
                val log = scala.math.log(normalized)
                log
        }
                .project('stemmedVenName, 'normalized)
        normalizedBuzz

    }

    def buzzScore(normalizedBuzz: RichPipe, min: RichPipe, max: RichPipe): RichPipe = {
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
    }.project('stemmedVenName, 'buzzScore)


}
