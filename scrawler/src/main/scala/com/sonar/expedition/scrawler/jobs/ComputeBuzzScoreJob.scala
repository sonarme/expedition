package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import com.twitter.scalding.Job

class ComputeBuzzScoreJob(args: Args) extends Job(args) with CheckinSource with CheckinGrouperFunction with BuzzFromCheckins with PlacesCorrelation {

    val output = args("output")
    val (_, venueIdCheckins) = checkinSource(args, true, true)
    val shinglesPipe = getShingles(venueIdCheckins)
    val buzz = findBuzz(shinglesPipe, venueIdCheckins)
    val buzzStats = findBuzzStats(buzz)
    val normalizedBuzz = normalizeBuzz(buzz, buzzStats)
    val buzzMin = findMin(normalizedBuzz)
    val buzzMax = findMax(normalizedBuzz)
    val buzzScore = calculateBuzzScore(normalizedBuzz, buzzMin, buzzMax).write(TextLine(output))

}
