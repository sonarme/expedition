package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import com.sonar.expedition.scrawler.pipes.PlacesCorrelation
import PlaceClassification._

class PlaceClassification(args: Args) extends Job(args) with PlacesCorrelation with CheckinSource {

    val bayesmodel = args("bayesmodelforvenuetype")
    val placesData = null
    //args("placesData")
    val output = args("placeClassification")

    val (checkinsInputPipe, _) = checkinSource(args, true, false)

    val placesVenueGoldenId = placeClassification(checkinsInputPipe, bayesmodel, placesData)
    placesVenueGoldenId.unique(PlaceClassificationOutputTuple)
            .write(SequenceFile(output, PlaceClassificationOutputTuple))
            .write(Tsv(output + "_tsv", PlaceClassificationOutputTuple))


}

object PlaceClassification {
    val PlaceClassificationOutputTuple = ('goldenId, 'venueId, 'venueLat, 'venueLng, 'venName, 'venAddress, 'venuePhone, 'venueType)
}

/*
 .groupBy('goldenId) {
         _.toList[(String, Double, Double, String, String)](('correlatedVenueIds, 'venueLat, 'venueLng, 'venName, 'venueType) -> 'venueDataList)
     }.flatMap('venueDataList ->('venueId, 'venueLat, 'venueLng, 'venName, 'venueTypes)) {
         groupData: List[(List[String], Double, Double, String, String)] =>
             val (correlatedVenueIds, lat, lng, venName, _) = groupData.head
             val venueTypes = groupData.flatMap {
                 case (_, _, _, _, venType) => if (CommonFunctions.isNullOrEmpty(venType)) None else Some(venType)
             }.distinct
             correlatedVenueIds map {
                 correlatedVenueId => (correlatedVenueId, lat, lng, venName, venueTypes)
             }
     }
}*/
