package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{RichPipe, TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import cascading.tuple.Fields

class BuzzFromCheckinsTest(args: Args) extends Job(args) {

    val checkinsWithMessage = args("checkinsWithMessage")
    val checkinsWithoutMessage = args("checkinsWithoutMessage")


    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val buzzPipe = new BuzzFromCheckins(args)

    val msgCheckins = checkinGrouperPipe.checkinsWithMessage(TextLine(checkinsWithMessage).read)
    val noMsgCheckins = checkinGrouperPipe.unfilteredCheckinsLatLon(TextLine(checkinsWithoutMessage).read)
    val shinglesPipe = buzzPipe.getShingles(msgCheckins)
    val buzz = buzzPipe.findBuzz(shinglesPipe, noMsgCheckins)
//            .map('singleShinglesize -> ('singleShinglesize, 'size)) {
//        fields: (String) =>
//            val (size1) = fields
//            val size2 = size1
//            (size1, size2)
//    }
//            .groupBy('singleShinglesize) {
//        _
//                .average('singleShinglesize -> 'avg)
//    }.map(('singleShinglesize, 'avg) -> 'normalized) {
//        fields: (String, String) =>
//            val (shingleSize, avg) = fields
//            val normalized = shingleSize.toDouble / avg.toDouble
//            normalized
//    }
            .project(Fields.ALL)
            .write(TextLine("/tmp/buzz.txt"))

}