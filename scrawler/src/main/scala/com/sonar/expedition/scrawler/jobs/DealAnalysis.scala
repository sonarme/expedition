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

    val crawls = SequenceFile(args("crawl"), CrawlAggregationJob.CrawlOutTuple).read.project('venueId, 'phone).rename(('venueId, 'phone) ->('crawlVenueId, 'crawlPhone))

    val deals = Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))
            // match multiple locations
            .flatMap('locationJSON ->('merchantLat, 'merchantLng, 'merchantGeosector, 'merchantAddress, 'merchantCity, 'merchantState, 'merchantZip, 'merchantPhone)) {
        locationJSON: String =>
            val dealLocations = try {
                DealAnalysis.DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, DealAnalysis.DealLocationsTypeReference)
            } catch {
                case e => throw new RuntimeException("JSON error:" + locationJSON, e)
            }
            for (dealLocation <- dealLocations;
                 geosector <- dealMatchGeosectorsAdjacent(dealLocation.latitude, dealLocation.longitude))
            yield (dealLocation.latitude, dealLocation.longitude, geosector, dealLocation.address, dealLocation.city, dealLocation.state, dealLocation.zip, dealLocation.phone)
    }.project(DealAnalysis.DealSheetTuple)

    val combined = args.optional("livingsocial") match {
        case Some(file) =>
            val ls = SequenceFile(file,
                ('url, 'timestamp, 'merchantName2, 'majorCategory2, 'rating, 'merchantLat2, 'merchantLng2, 'merchantAddress2, 'merchantCity2, 'merchantState2, 'merchantZip2, 'merchantPhone2, 'priceRange, 'reviewCount, 'likes, 'dealDescription, 'dealImage, 'dealRegion, 'minPricepoint2, 'purchased, 'savingsPercent)).read
                    .filter('minPricepoint2) {
                price: Int => price > 0
            }
                    .flatMap(('url, 'merchantLat2, 'merchantLng2) ->('dealId2, 'successfulDeal2, 'merchantGeosector2, 'minorCategory2)) {
                in: (String, Double, Double) =>
                    val (url, lat, lng) = in
                    val dealId = url.split('/').last.split('-').head
                    for (geosector <- dealMatchGeosectorsAdjacent(lat, lng))
                    yield (dealId, "?", geosector, "?")
            }
            ls.leftJoinWithTiny('dealId2 -> 'dealId, deals).map(DealAnalysis.DealSheetTuple.append(DealAnalysis.DealSheetTuple2) -> DealAnalysis.DealSheetTuple) {
                in: Tuple =>
                    val out = pickBest(in, DealAnalysis.DealSheetTuple.size())
                    out
            }
        case _ => deals.map(() -> DealAnalysis.LsCrawlSpecialTuple) {
            u: Unit => ("?", "?", "?", "?", "?", "?", "?")
        }
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
            .leftJoinWithSmaller('venueId -> 'crawlVenueId, crawls).discard('crawlVenueId)
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
        _.sortedTake[Int](('levenshtein) -> 'topVenueMatch, 1).head(DealAnalysis.DealsOutputTupleWithoutId -> DealAnalysis.DealsOutputTupleWithoutId)

    }.map(('venueLat, 'venueLng) -> 'venueSector) {
        in: (Double, Double) => GeoHash.withCharacterPrecision(in._1, in._2, 8).toBase32
    }.map('merchantAddress -> 'merchantAddress) {
        merchantAddress: String =>
            if (merchantAddress == null) null else merchantAddress.replaceAll("\\s", " ")
    }
            .write(SequenceFile(dealsOutput, DealAnalysis.DealsOutputTuple.append('venueSector)))
            .write(Tsv(dealsOutput + "_tsv", DealAnalysis.DealsOutputTuple.append('venueSector)))


    def pickBest(tuple: Tuple, num: Int) = {
        val (left, right) = tuple.splitAt(num)
        new Tuple((left zip right map {
            case (l, r) => Option(l).getOrElse(r)
        }).toSeq: _*)
    }
}

import collection.JavaConversions._

object DealAnalysis extends FieldConversions {
    val DealSheetTuple = ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'merchantLat, 'merchantLng, 'merchantGeosector, 'merchantAddress, 'merchantCity, 'merchantState, 'merchantZip, 'merchantPhone)

    def doubleFields(fields: Fields) = {
        val fieldsSeq = fields.iterator().map(_.toString).toSeq
        new Fields(fieldsSeq.map(_ + "2"): _*)
    }

    val DealSheetTuple2 = doubleFields(DealSheetTuple)
    val LsCrawlSpecialTuple = ('dealRegion, 'rating, 'priceRange, 'reviewCount, 'likes, 'purchased, 'savingsPercent)
    val DealsOutputTuple = ('dealId, 'successfulDeal, 'goldenId, 'venName, 'venueLat, 'venueLng, 'merchantName, 'merchantAddress, 'merchantCity, 'merchantState, 'merchantZip, 'merchantPhone, 'majorCategory, 'minorCategory, 'minPricepoint) append LsCrawlSpecialTuple
    val DealsOutputTupleWithoutId = DealsOutputTuple.subtract('dealId)
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
