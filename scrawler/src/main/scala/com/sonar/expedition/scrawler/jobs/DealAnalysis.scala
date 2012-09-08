package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.fasterxml.jackson.databind.{DeserializationFeature, DeserializationConfig, ObjectMapper}
import java.util
import com.sonar.expedition.scrawler.util._
import ch.hsr.geohash.{WGS84Point, GeoHash}
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
import com.sonar.expedition.scrawler.jobs.DealLocation
import com.twitter.scalding.SequenceFile
import scala.Some
import com.twitter.scalding.Tsv
import ch.hsr.geohash.util.VincentyGeodesy

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

    val crawls = SequenceFile(args("crawl"), CrawlAggregationJob.CrawlOutTuple).read.project('venueId, 'phone).rename(('venueId, 'phone) ->('crawlVenueId, 'crawlPhone))

    val deals = Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))
            // match multiple locations
            .flatMap('locationJSON ->('merchantLat, 'merchantLng, 'merchantGeosector, 'merchantAddress, 'merchantPhone)) {
        locationJSON: String =>
            val dealLocations = try {
                DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, DealLocationsTypeReference)
            } catch {
                case e => throw new RuntimeException("JSON error:" + locationJSON, e)
            }
            dealLocations map {
                dealLocation =>
                    (dealLocation.latitude, dealLocation.longitude, dealMatchGeosector(dealLocation.latitude, dealLocation.longitude), dealLocation.address, dealLocation.phone)
            }
    }.discard('locationJSON)

    val dealVenues = SequenceFile(placeClassification, PlaceClassification.PlaceClassificationOutputTuple).map(('venueLat, 'venueLng) -> 'geosector) {
        in: (Double, Double) =>
            val (lat, lng) = in
            dealMatchGeosector(lat, lng)
    }
            .joinWithTiny('geosector -> 'merchantGeosector, deals)
            .leftJoinWithSmaller('venueId -> 'crawlVenueId, crawls).discard('crawlVenueId)
            .flatMap(('venName, 'venueLat, 'venueLng, 'venAddress, 'crawlPhone, 'merchantName, 'merchantLat, 'merchantLng, 'merchantAddress, 'merchantPhone) ->('levenshtein, 'distance)) {
        in: (String, Double, Double, String, String, String, Double, Double, String, String) =>
            val (venName, venueLat, venueLng, venAddress, venuePhone, merchantName, merchantLat, merchantLng, merchantAddress, merchantPhone) = in
            val stemmedVenName = StemAndMetaphoneEmployer.removeStopWords(venName)
            val stemmedMerchantName = StemAndMetaphoneEmployer.removeStopWords(merchantName)
            lazy val levenshtein = Levenshtein.compareInt(stemmedVenName, stemmedMerchantName)
            val distance = Haversine.distanceInMeters(new WGS84Point(venueLat, venueLng), new WGS84Point(merchantLat, merchantLng))
            if (distance > distanceArg || levenshtein > math.min(stemmedVenName.length, stemmedMerchantName.length) * levenshteinFactor) None
            else {
                val houseNumber = extractFirstNumber(venAddress)
                val phone = stripPhone(venuePhone)
                val score = if (houseNumber.isDefined && houseNumber == extractFirstNumber(merchantAddress)
                        || phone.isDefined && phone == stripPhone(merchantPhone)) -1
                else levenshtein
                Some((score, distance))
            }
    }.groupBy('dealId) {
        _.sortedTake[Int](('levenshtein) -> 'topVenueMatch, 1).head('successfulDeal, 'goldenId, 'venName, 'venAddress, 'venuePhone, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'merchantLat, 'merchantLng, 'merchantAddress, 'merchantPhone, 'distance, 'levenshtein)
        /* _.max('levenshtein -> 'maxLevenshtein)
            .max('distance -> 'maxDistance)
            .sortWithTake[(Double,Double,Double,Double)](('levenshtein, 'maxLevenshtein,'distance, 'maxDistance)  -> 'topVenueMatch, 1) {
        (in1: (Double,Double,Double,Double), in2:  (Double,Double,Double,Double)) =>
           distanceCalc(in1) < distanceCalc(in2)
    }.head('goldenId, 'venName, 'merchantName, 'distance, 'levenshtein)*/
    } // TODO: need to dedupe venues here
    dealVenues.map(() -> 'enabled) {
        u: Unit => true
    }
            .write(Tsv(dealsOutput, DealsOutputTuple))
}

object DealAnalysis {
    val DealsOutputTuple = ('enabled, 'dealId, 'successfulDeal, 'goldenId, 'venName, 'venAddress, 'venuePhone, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'merchantLat, 'merchantLng, 'merchantAddress, 'merchantPhone, 'distance, 'levenshtein)
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
