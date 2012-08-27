package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import com.twitter.scalding.Job

class BuzzFromCheckinsTest(args: Args) extends Job(args) with CheckinGrouperFunction with BuzzFromCheckins with PlacesCorrelation {

    val checkinsWithoutMessage = args("checkinsWithoutMessage")
    val checkinsWithVenueId = args("checkinsWithVenueId")
    val output = args("output")

    val venueId = correlationCheckins(TextLine(checkinsWithVenueId).read)
    val msgCheckins = checkinsWithMessage(TextLine(checkinsWithVenueId).read)
    val noMsgCheckins = unfilteredCheckinsLatLon(TextLine(checkinsWithoutMessage).read)
    val venueIdCheckins = withGoldenId(noMsgCheckins, venueId)
    val shinglesPipe = getShingles(msgCheckins)
    val buzz = findBuzz(shinglesPipe, venueIdCheckins)
    val buzzStats = findBuzzStats(buzz)
    val normalizedBuzz = normalizeBuzz(buzz, buzzStats)
    val buzzMin = findMin(normalizedBuzz)
    val buzzMax = findMax(normalizedBuzz)
    val buzzScore = calculateBuzzScore(normalizedBuzz, buzzMin, buzzMax).write(TextLine(output))

}
