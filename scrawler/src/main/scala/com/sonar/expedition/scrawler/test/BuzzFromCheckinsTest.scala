package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{RichPipe, TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import cascading.tuple.Fields

class BuzzFromCheckinsTest (args: Args) extends Job(args) {

    val checkinsWithMessage = args("checkinsWithMessage")
    val checkinsWithoutMessage = args("checkinsWithoutMessage")


    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val buzzPipe = new BuzzFromCheckins(args)

    val msgCheckins = checkinGrouperPipe.checkinsWithMessage(TextLine(checkinsWithMessage).read)
    val noMsgCheckins = checkinGrouperPipe.unfilteredCheckinsLatLon(TextLine(checkinsWithoutMessage).read)
    val shinglesPipe = buzzPipe.getShingles(msgCheckins)
    val buzz = buzzPipe.findBuzz(shinglesPipe, noMsgCheckins).project(Fields.ALL)
    .write(TextLine("/tmp/buzz.txt"))

}