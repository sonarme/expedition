package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}

class BuzzFromCheckinsTest(args: Args) extends Job(args) {

    val checkinsWithMessage = args("checkinsWithMessage")
    val checkinsWithoutMessage = args("checkinsWithoutMessage")
    val checkinsWithVenueId = args("checkinsWithVenueId")
    val output = args("output")

    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val buzzPipe = new BuzzFromCheckins(args)
    val venueIdPipe = new PlacesCorrelation(args)

    val venueId = checkinGrouperPipe.correlationCheckins(TextLine(checkinsWithVenueId).read)
    val msgCheckins = checkinGrouperPipe.checkinsWithMessage(TextLine(checkinsWithMessage).read)
    val noMsgCheckins = checkinGrouperPipe.unfilteredCheckinsLatLon(TextLine(checkinsWithoutMessage).read)
    val venueIdCheckins = venueIdPipe.withGoldenId(noMsgCheckins, venueId)
    val shinglesPipe = buzzPipe.getShingles(msgCheckins)
    val buzz = buzzPipe.findBuzz(shinglesPipe, venueIdCheckins)
    val buzzStats = buzzPipe.findBuzzStats(buzz)
    val normalizedBuzz = buzzPipe.normalizedBuzz(buzz, buzzStats)
    val buzzMin = buzzPipe.findMin(normalizedBuzz)
    val buzzMax = buzzPipe.findMax(normalizedBuzz)
    val buzzScore = buzzPipe.buzzScore(normalizedBuzz, buzzMin, buzzMax).write(TextLine(output))

}