package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Job, Args, RichPipe}
import com.sonar.expedition.scrawler.util.{StemAndMetaphoneEmployer, CommonFunctions, ShingleTokenizer}
import org.apache.commons.lang.StringUtils
import cascading.pipe.joiner.LeftJoin

class BuzzFromCheckins(args: Args) extends Job(args) {


    def getShingles(checkinsWithMessage: RichPipe): RichPipe = {
        val messagePipe = checkinsWithMessage
        //        val withoutMessagePipe = checkinsWithoutMessage   , checkinsWithoutMessage: RichPipe

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


}
