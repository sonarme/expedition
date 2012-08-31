package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, CheckinGrouperFunction, BuzzFromCheckins}
import com.twitter.scalding.SequenceFile

class CountDealAnalysisCheckins(args: Args) extends Job(args) with CheckinSource with CheckinGrouperFunction with BuzzFromCheckins with PlacesCorrelation {

    val dealAnalysisInput = args("dealAnalysisInput")
    val dealsCheckinCount = args("dealsCheckinCount")
    val (_, venueIdCheckins) = checkinSource(args, true, true)
    SequenceFile(dealAnalysisInput, ('dealId, 'goldenId, 'venName, 'merchantName, 'levenshtein)).read
            .project('goldenId, 'merchantName)
            .rename('goldenId -> 'dealGoldenId)
            .joinWithLarger('dealGoldenId -> 'goldenId, venueIdCheckins.project('goldenId))
            .groupBy('merchantName, 'goldenId) {
        _.size
    }.write(SequenceFile(dealsCheckinCount, ('merchantName, 'goldenId, 'size)))
            .write(Tsv(dealsCheckinCount + "_tsv", ('merchantName, 'goldenId, 'size)))

}
