package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}

class BuzzFromCheckinsTest(args: Args) extends Job(args) {

    val checkinsWithMessage = args("checkinsWithMessage")
    val checkinsWithoutMessage = args("checkinsWithoutMessage")
    val checkinsWithVenueId = args("checkinsWithVenueId")

    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val buzzPipe = new BuzzFromCheckins(args)
    val venueIdPipe = new PlacesCorrelation(args)


    val venueId = checkinGrouperPipe.correlationCheckins(TextLine(checkinsWithVenueId).read)
    val msgCheckins = checkinGrouperPipe.checkinsWithMessage(TextLine(checkinsWithMessage).read)
    val noMsgCheckins = checkinGrouperPipe.unfilteredCheckinsLatLon(TextLine(checkinsWithoutMessage).read)
    val venueIdCheckins = venueIdPipe.withGoldenId(noMsgCheckins, venueId).write(TextLine("/tmp/venueId.txt"))
    val shinglesPipe = buzzPipe.getShingles(msgCheckins).write(TextLine("/tmp/shingles.txt"))
    val buzz = buzzPipe.findBuzz(shinglesPipe, venueIdCheckins).write(TextLine("/tmp/buzz.txt"))
    val buzzStats = buzzPipe.findBuzzStats(buzz).write(TextLine("/tmp/buzzStats.txt"))
    val normalizedBuzz = buzzPipe.normalizedBuzz(buzz, buzzStats).write(TextLine("/tmp/nomrmalbuzz.txt"))
    val buzzMin = buzzPipe.findMin(normalizedBuzz).write(TextLine("/tmp/buzzMin.txt"))
    val buzzMax = buzzPipe.findMax(normalizedBuzz).write(TextLine("/tmp/buzzMax.txt"))
    val buzzScore = buzzPipe.buzzScore(normalizedBuzz, buzzMin, buzzMax).write(TextLine("/tmp/buzzScore.txt"))

}