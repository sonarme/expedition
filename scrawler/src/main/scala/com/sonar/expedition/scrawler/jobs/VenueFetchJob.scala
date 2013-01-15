package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import java.util
import com.sonar.expedition.scrawler.util._
import DealAnalysis._
import com.sonar.expedition.scrawler.pipes.{CheckinGrouperFunction, PlacesCorrelation}
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import ch.hsr.geohash.util.VincentyGeodesy
import ch.hsr.geohash.WGS84Point

class VenueFetchJob(args: Args) extends DefaultJob(args) with PlacesCorrelation with CheckinGrouperFunction with CheckinSource {
    val placeClassification = args("placeClassification")
    val dealsInput = args("dealsInput")
    val venueOutput = args("venueOutput")
    val distanceArg = args.getOrElse("distance", "250").toInt

    val deals = Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))
            // match multiple locations
            .flatMap('locationJSON ->('merchantLat, 'merchantLng, 'merchantGeosector)) {
        locationJSON: String =>
            val dealLocations = try {
                DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, DealLocationsTypeReference)
            } catch {
                case e => throw new RuntimeException("JSON error:" + locationJSON, e)
            }
            dealLocations map {
                dealLocation =>
                    (dealLocation.latitude, dealLocation.longitude, dealMatchGeosector(dealLocation.latitude, dealLocation.longitude))
            }
    }
    SequenceFile(placeClassification, PlaceClassification.PlaceClassificationOutputTuple).map(('venueLat, 'venueLng) -> 'geosector) {
        in: (Double, Double) =>
            val (lat, lng) = in
            dealMatchGeosector(lat, lng)
    }
            .joinWithTiny('geosector -> 'merchantGeosector, deals)
            .filter('venName, 'venueLat, 'venueLng, 'merchantName, 'merchantLat, 'merchantLng) {
        in: (String, Double, Double, String, Double, Double) =>
            val (venName, venueLat, venueLng, merchantName, merchantLat, merchantLng) = in
            val stemmedVenName = StemAndMetaphoneEmployer.removeStopWords(venName)
            val stemmedMerchantName = StemAndMetaphoneEmployer.removeStopWords(merchantName)
            lazy val levenshtein = Levenshtein.compareInt(stemmedVenName, stemmedMerchantName)
            val distance = Haversine.distanceInMeters(new WGS84Point(venueLat, venueLng), new WGS84Point(merchantLat, merchantLng))
            distance <= distanceArg && levenshtein <= math.min(stemmedVenName.length, stemmedMerchantName.length) * 0.33
    }.unique('venueId)
            .write(Tsv(venueOutput, Fields.ALL))
}
