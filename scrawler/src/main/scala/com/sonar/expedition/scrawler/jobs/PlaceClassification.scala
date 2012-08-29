package com.sonar.expedition.scrawler.jobs

import cascading.tuple.Fields
import com.twitter.scalding.{Tsv, Job, Args}
import com.sonar.expedition.scrawler.pipes.PlacesCorrelation

class PlaceClassification(args: Args) extends Job(args) with PlacesCorrelation with CheckinSource {

    val bayestrainingmodel = args("bayestrainingmodelforlocationtype")
    val placesData = args("placesData")
    val output = args("placesOutput")

    val checkinsInputPipe = checkinSource(args, withVenuesOnly = true)

    val placesVenueGoldenId = placeClassification(checkinsInputPipe, bayestrainingmodel, placesData)
    val similarity = placesVenueGoldenId.groupBy('goldenId) {
        _.toList[(String, String)](('venName, 'venueType) -> 'venueDataList)
    }.map('venueDataList ->('venName, 'venueTypes)) {
        groupData: List[(String, String)] =>
            val venName = groupData.head._1
            val venueTypes = groupData.map(_._2)
            (venName, venueTypes)
    }.discard('venueDataList)
            .write(Tsv(output, Fields.ALL))


}

