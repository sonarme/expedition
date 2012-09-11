package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.fasterxml.jackson.databind.{DeserializationFeature, DeserializationConfig, ObjectMapper}
import java.util
import com.sonar.expedition.scrawler.util._
import ch.hsr.geohash.{WGS84Point, GeoHash}
import reflect.BeanProperty
import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import com.sonar.expedition.scrawler.pipes.{CheckinGrouperFunction, PlacesCorrelation}
import cascading.tuple.{Tuple, Fields}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv

class DealAnalysis(args: Args) extends Job(args) with PlacesCorrelation with CheckinGrouperFunction with CheckinSource {
    val placeClassification = args("placeClassification")
    val dealsInput = args("dealsInput")
    val dealsOutput = args("dealsOutput")
    val distanceArg = args.getOrElse("distance", "250").toInt
    val levenshteinFactor = args.getOrElse("levenshteinFactor", "0.33").toDouble
    val firstNumber = """\s*(\d+)[^\d]*""".r

    def distanceCalc(in: (Double, Double, Double, Double)) = {
        val (levenshtein, maxLevenshtein, distance, maxDistance) = in
        (levenshtein / maxLevenshtein) * (distance / maxDistance)
    }

    def extractFirstNumber(s: String) = s match {
        case firstNumber(numStr) => Some(numStr)
        case _ => None
    }


    def stripPhone(s: String) = if (s == null || s == "") None else Some(s.replaceAllLiterally("+1", "").replaceAllLiterally("-", "").replaceAllLiterally(" ", ""))

    val crawls = SequenceFile(args("crawl"), CrawlAggregationJob.CrawlOutTuple).read.project('venueId, 'phone, 'checkins, 'category).rename('phone -> 'crawlPhone).map(('venueId, 'category) -> 'foursquareCategory) {
        in: (String, String) => if (in._1.startsWith("foursquare")) in._2 else null
    }

    val deals = Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))
            // match multiple locations
            .flatMap('locationJSON ->('merchantLat, 'merchantLng, 'merchantGeosector, 'merchantAddress, 'merchantCity, 'merchantZip, 'merchantState, 'merchantPhone)) {
        locationJSON: String =>
            val dealLocations = try {
                DealAnalysis.DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, DealAnalysis.DealLocationsTypeReference)
            } catch {
                case e => throw new RuntimeException("JSON error:" + locationJSON, e)
            }
            for (dealLocation <- dealLocations)
            yield (dealLocation.latitude, dealLocation.longitude, dealMatchGeosector(dealLocation.latitude, dealLocation.longitude), dealLocation.address, dealLocation.city, dealLocation.zip, dealLocation.state, dealLocation.phone)
    }.discard('locationJSON).map(() -> DealAnalysis.LsCrawlSpecialTuple) {
        u: Unit => (null, null, null, null, null, null)
    }

    val ls = SequenceFile(args("livingsocial"), ('url, 'timestamp, 'merchantName, 'majorCategory, 'rating, 'merchantLat, 'merchantLng, 'merchantAddress, 'merchantCity, 'merchantZip, 'merchantState, 'merchantPhone, 'priceRange, 'reviewCount, 'likes, 'dealDescription, 'dealImage, 'minPricepoint, 'purchased, 'savingsPercent)).read
            .filter('minPricepoint) {
        price: Int => price > 0
    }
            .map(('url, 'merchantLat, 'merchantLng) ->('dealId, 'successfulDeal, 'merchantGeosector, 'minorCategory)) {
        in: (String, Double, Double) =>
            val (url, lat, lng) = in
            val dealId = url.split('/').last.split(Array('-', '?')).head
            (dealId, "x?", dealMatchGeosector(lat, lng), "?")
    }.project(('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'merchantLat, 'merchantLng, 'merchantGeosector, 'merchantAddress, 'merchantCity, 'merchantZip, 'merchantState, 'merchantPhone).append(DealAnalysis.LsCrawlSpecialTuple))
    val combined = (deals ++ ls).groupBy('dealId, 'merchantGeosector) {
        _.sortBy('successfulDeal).head('successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'merchantLat, 'merchantLng, 'merchantAddress, 'merchantCity, 'merchantZip, 'merchantState, 'merchantPhone, 'rating, 'priceRange, 'reviewCount, 'likes, 'purchased, 'savingsPercent)
    }.discard('merchantGeosector).flatMap(('merchantLat, 'merchantLng) -> 'merchantGeosector) {
        in: (Double, Double) =>
            val (lat, lng) = in
            dealMatchGeosectorsAdjacent(lat, lng)
    }

    val dealVenues = SequenceFile(placeClassification, PlaceClassification.PlaceClassificationOutputTuple).map(('venueLat, 'venueLng) -> 'geosector) {
        in: (Double, Double) =>
            val (lat, lng) = in
            dealMatchGeosector(lat, lng)
    }
            .joinWithSmaller('geosector -> 'merchantGeosector, combined)
            .map(('venueLat, 'venueLng, 'merchantLat, 'merchantLng) -> 'distance) {
        in: (Double, Double, Double, Double) =>
            val (venueLat, venueLng, merchantLat, merchantLng) = in
            Haversine.haversineInMeters(venueLat, venueLng, merchantLat, merchantLng)
    }.filter('distance) {
        distance: Int => distance <= distanceArg
    }
            .leftJoinWithSmaller('venueId -> 'crawlVenueId, crawls.rename('venueId -> 'crawlVenueId)).discard('crawlVenueId)
            .flatMap(('venName, 'venAddress, 'crawlPhone, 'merchantName, 'merchantAddress, 'merchantPhone) -> 'levenshtein) {
        in: (String, String, String, String, String, String) =>
            val (venName, venAddress, venuePhone, merchantName, merchantAddress, merchantPhone) = in
            val stemmedVenName = StemAndMetaphoneEmployer.removeStopWords(venName)
            val stemmedMerchantName = StemAndMetaphoneEmployer.removeStopWords(merchantName)
            lazy val levenshtein = Levenshtein.compareInt(stemmedVenName, stemmedMerchantName)
            if (levenshtein > math.min(stemmedVenName.length, stemmedMerchantName.length) * levenshteinFactor) None
            else {
                val houseNumber = extractFirstNumber(venAddress)
                val phone = stripPhone(venuePhone)
                val score = if (houseNumber.isDefined && houseNumber == extractFirstNumber(merchantAddress)
                        || phone.isDefined && phone == stripPhone(merchantPhone)) -1
                else levenshtein
                Some(score)
            }
    }.groupBy('dealId) {
        _.sortBy('levenshtein).head(DealAnalysis.DealsDataTuple -> DealAnalysis.DealsDataTuple)

    }.map(('venueLat, 'venueLng) -> 'venueSector) {
        in: (Double, Double) => GeoHash.withCharacterPrecision(in._1, in._2, 8).toBase32
    }.map('merchantAddress -> 'merchantAddress) {
        merchantAddress: String =>
            if (merchantAddress == null) null else merchantAddress.replaceAll("\\s", " ")
    }
            .write(SequenceFile(dealsOutput, DealAnalysis.DealsOutputTuple))
            .write(Tsv(dealsOutput + "_tsv", DealAnalysis.DealsOutputTuple))

}

import collection.JavaConversions._

object DealAnalysis extends FieldConversions {
    val LsCrawlSpecialTuple = ('rating, 'priceRange, 'reviewCount, 'likes, 'purchased, 'savingsPercent)
    val DealsDataTuple = ('successfulDeal, 'goldenId, 'venName, 'venueLat, 'venueLng, 'merchantName, 'merchantAddress, 'merchantCity, 'merchantZip, 'merchantState, 'merchantPhone, 'majorCategory, 'minorCategory, 'minPricepoint, 'checkins, 'foursquareCategory) append LsCrawlSpecialTuple
    val DealsOutputTuple = ('dealId).append(DealsDataTuple).append('venueSector)
    val DealObjectMapper = new ObjectMapper
    DealObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    val DealLocationsTypeReference = new TypeReference[util.List[DealLocation]] {}
}

case class DealLocation(
                               @BeanProperty address: String,
                               @BeanProperty city: String = null,
                               @BeanProperty state: String = null,
                               @BeanProperty zip: String = null,
                               @BeanProperty phone: String = null,
                               @BeanProperty latitude: Double = 0,
                               @BeanProperty longitude: Double = 0
                               ) {
    def this() = this(null)
}
