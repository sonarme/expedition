package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.Tuples

class PlaceExtraction(args: Args) extends Job(args) with CheckinSource {
    val placeOutput = args("output")
    val (checkinsWithVenues, _) = checkinSource(args, true, false)
    checkinsWithVenues.groupBy('serType, 'venId) {
        _.head('venName, 'venAddress, 'lat, 'lng)
    }.write(SequenceFile(placeOutput, Tuples.Place))
            .limit(180000).write(SequenceFile(placeOutput + "_small", Tuples.Place))
}
