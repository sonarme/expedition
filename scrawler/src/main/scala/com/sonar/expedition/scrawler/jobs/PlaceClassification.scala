package com.sonar.expedition.scrawler.jobs

import cascading.tuple.Fields
import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import com.sonar.expedition.scrawler.pipes.PlacesCorrelation
import com.sonar.expedition.scrawler.util.CommonFunctions

class PlaceClassification(args: Args) extends Job(args) with PlacesCorrelation with CheckinSource {

    val bayestrainingmodel = args("bayestrainingmodelforlocationtype")
    val placesData = args("placesData")
    val output = args("placesOutput")

    val checkinsInputPipe = checkinSource(args, withVenuesOnly = true)

    val placesVenueGoldenId = placeClassification(checkinsInputPipe, bayestrainingmodel, placesData)
    val similarity = placesVenueGoldenId.groupBy('goldenId) {
        _.toList[(List[(String, String)], Double, Double, String, String)](('correlatedVenueIds, 'lat, 'lng, 'venName, 'venueType) -> 'venueDataList)
    }.map('venueDataList ->('correlatedVenueIds, 'lat, 'lng, 'venName, 'venueTypes)) {
        groupData: List[(List[(String, String)], Double, Double, String, String)] =>
            val (correlatedVenueIds, lat, lng, venName, _) = groupData.head
            val venueTypes = groupData.flatMap {
                case (_, _, _, _, venType) => if (CommonFunctions.isNullOrEmpty(venType)) None else Some(venType)
            }
            (correlatedVenueIds, lat, lng, venName, venueTypes)
    }.discard('venueDataList)
            .write(SequenceFile(output, Fields.ALL))


}

