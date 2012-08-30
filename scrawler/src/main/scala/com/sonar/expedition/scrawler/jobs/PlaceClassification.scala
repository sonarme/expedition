package com.sonar.expedition.scrawler.jobs

import cascading.tuple.Fields
import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import com.sonar.expedition.scrawler.pipes.PlacesCorrelation
import com.sonar.expedition.scrawler.util.CommonFunctions

class PlaceClassification(args: Args) extends Job(args) with PlacesCorrelation with CheckinSource {

    val bayestrainingmodel = args("bayestrainingmodelforvenuetype")
    val placesData = args("placesData")
    val output = args("placeClassification")

    val (checkinsInputPipe, _) = checkinSource(args, true, false)

    val placesVenueGoldenId = placeClassification(checkinsInputPipe, bayestrainingmodel, placesData)
            .write(SequenceFile(output, ('goldenId, 'venueId, 'venueLat, 'venueLng, 'venName, 'venueType)))
            .write(Tsv(output + "_tsv", ('goldenId, 'venueId, 'venueLat, 'venueLng, 'venName, 'venueType)))


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
