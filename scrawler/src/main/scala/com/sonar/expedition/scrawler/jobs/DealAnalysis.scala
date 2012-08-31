package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.fasterxml.jackson.databind.{DeserializationFeature, DeserializationConfig, ObjectMapper}
import java.util
import com.sonar.expedition.scrawler.util.{Haversine, Levenshtein, CommonFunctions, StemAndMetaphoneEmployer}
import ch.hsr.geohash.GeoHash
import DealAnalysis._
import reflect.BeanProperty
import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import com.sonar.expedition.scrawler.pipes.{CheckinGrouperFunction, PlacesCorrelation}
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.Tsv
import com.sonar.scalding.cassandra.NarrowRowScheme
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.Tsv
import com.twitter.scalding.TextLine
import com.sonar.scalding.cassandra.NarrowRowScheme
import util.Date
import java.text.SimpleDateFormat
import cascading.pipe.Pipe
import com.fasterxml.jackson.core.`type`.TypeReference

class DealAnalysis(args: Args) extends Job(args) with PlacesCorrelation with CheckinGrouperFunction with CheckinSource {
    val placeClassification = args("placeClassification")
    val dealsInput = args("dealsInput")
    val dealsOutput = args("dealsOutput")
    val dealMatchGeosectorSize = args.getOrElse("geosectorSize", "10").toInt
    val distance = args.getOrElse("distance", "300").toInt
    val levenshteinFactor = args.getOrElse("levenshteinFactor", "0.4").toDouble

    def dealMatchGeosector(lat: Double, lng: Double) = GeoHash.withBitPrecision(lat, lng, dealMatchGeosectorSize).longValue()

    val deals = Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))
            // match multiple locations
            .flatMap(('merchantName, 'locationJSON) ->('stemmedMerchantName, 'merchantLat, 'merchantLng, 'merchantGeosector)) {
        in: (String, String) =>
            val (merchantName, locationJSON) = in
            val dealLocations = try {
                DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, DealLocationsTypeReference)
            } catch {
                case e => throw new RuntimeException("JSON error:" + locationJSON, e)
            }
            val stemmedMerchantName = StemAndMetaphoneEmployer.removeStopWords(merchantName)
            dealLocations map {
                dealLocation =>
                    (stemmedMerchantName, dealLocation.latitude, dealLocation.longitude, dealMatchGeosector(dealLocation.latitude, dealLocation.longitude))
            }
    }.discard('locationJSON)

    val dealVenues = SequenceFile(placeClassification, ('goldenId, 'venueId, 'venueLat, 'venueLng, 'venName, 'venueTypes)).map(('venueLat, 'venueLng, 'venName) ->('geosector, 'stemmedVenName)) {
        in: (Double, Double, String) =>
            val (lat, lng, venName) = in
            (dealMatchGeosector(lat, lng), StemAndMetaphoneEmployer.removeStopWords(venName))
    }
            .joinWithTiny('geosector -> 'merchantGeosector, deals)
            .flatMap(('stemmedVenName, 'venueLat, 'venueLng, 'stemmedMerchantName, 'merchantLat, 'merchantLng) ->('levenshtein, 'distance, 'score)) {
        in: (String, Double, Double, String, Double, Double) =>
            val (stemmedVenName, venueLat, venueLng, stemmedMerchantName, merchantLat, merchantLng) = in
            val levenshtein = Levenshtein.compareInt(stemmedVenName, stemmedMerchantName)
            //todo: use a lower geohash bit-depth and then compare the distance between lat/lng so that we filter out any venue candidates that aren't within a couple hundred meters
            lazy val distance = Haversine.haversine(venueLat, venueLng, merchantLat, merchantLng)
            if (levenshtein > math.min(stemmedVenName.length, stemmedMerchantName.length) * levenshteinFactor || distance > distance) None else Some((levenshtein, distance, levenshtein * distance))
    }.groupBy('dealId) {
        _.sortedTake[Int]('score -> 'topVenueMatch, 1).head('goldenId, 'venName, 'merchantName, 'distance, 'levenshtein, 'score)
    } // TODO: need to dedupe venues here
    dealVenues
            .write(SequenceFile(dealsOutput, ('dealId, 'goldenId, 'venName, 'merchantName, 'distance, 'levenshtein, 'score)))
            .write(Tsv(dealsOutput + "_tsv", ('dealId, 'goldenId, 'venName, 'merchantName, 'distance, 'levenshtein, 'score)))
}

object DealAnalysis {
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
