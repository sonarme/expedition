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
    // grouping venue types together
    val placeClassification = placesVenueGoldenId.groupBy('goldenId) {
        _.toList[(List[(String, String)], Double, Double, String, String)](('correlatedVenueIds, 'venueLat, 'venueLng, 'venName, 'venueType) -> 'venueDataList)
    }.flatMap('venueDataList ->('correlatedVenueId, 'venueLat, 'venueLng, 'venName, 'venueTypes)) {
        groupData: List[(List[String], Double, Double, String, String)] =>
            val (correlatedVenueIds, lat, lng, venName, _) = groupData.head
            val venueTypes = groupData.flatMap {
                case (_, _, _, _, venType) => if (CommonFunctions.isNullOrEmpty(venType)) None else Some(venType)
            }.distinct
            correlatedVenueIds map {
                correlatedVenueId => (correlatedVenueId, lat, lng, venName, venueTypes)
            }
    }.discard('venueDataList)
            .write(Tsv(output, Fields.ALL))


}

