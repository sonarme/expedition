package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile

class ComputeBuzzScoreJob(args: Args) extends Job(args) with CheckinSource with CheckinGrouperFunction with BuzzFromCheckins with PlacesCorrelation {

    val output = args("output")
    val (_, venueIdCheckins) = checkinSource(args, true, true)
    val shinglesPipe = getShingles(venueIdCheckins)
    val buzz = findBuzz(shinglesPipe, venueIdCheckins)
    val buzzStats = findBuzzStats(buzz)
    val scores = calculateBuzzScore(buzz, buzzStats)

    scores.write(SequenceFile(output, ('rowKey, 'columnName, 'columnValue)))
            .write(Tsv(output + "_tsv", ('rowKey, 'columnName, 'columnValue)))
}
