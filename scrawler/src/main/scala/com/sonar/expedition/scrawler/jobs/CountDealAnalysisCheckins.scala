package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import com.twitter.scalding.SequenceFile

class CountDealAnalysisCheckins(args: Args) extends DefaultJob(args) with CheckinSource with CheckinGrouperFunction with BuzzFromCheckins with PlacesCorrelation {

    val dealAnalysisInput = args("dealAnalysisInput")
    val dealsCheckinCount = args("dealsCheckinCount")
    val (_, venueIdCheckins) = checkinSource(args, true, true)
    SequenceFile(dealAnalysisInput, DealAnalysis.DealsOutputTuple).read
            .rename('goldenId -> 'dealGoldenId)
            .joinWithLarger('dealGoldenId -> 'goldenId, venueIdCheckins.project('goldenId))
            .groupBy('dealId, 'goldenId, 'venName, 'merchantName) {
        _.size
    }.write(SequenceFile(dealsCheckinCount, ('dealId, 'goldenId, 'venName, 'merchantName, 'size)))
            .write(Tsv(dealsCheckinCount + "_tsv", ('dealId, 'goldenId, 'venName, 'merchantName, 'size)))

}
