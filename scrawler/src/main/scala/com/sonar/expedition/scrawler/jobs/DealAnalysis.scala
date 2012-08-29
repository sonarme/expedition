package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.`type`.TypeReference
import java.util
import com.sonar.expedition.scrawler.util.{Levenshtein, CommonFunctions, StemAndMetaphoneEmployer}
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

class DealAnalysis(args: Args) extends Job(args) with PlacesCorrelation with CheckinGrouperFunction with CheckinSource {
    val dealsInput = args("dealsInput")
    val dealsOutput = args("dealsOutput")
    val checkins: Pipe = checkinSource(args, withVenuesOnly = true)

    val deals = Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON)).map(('merchantName, 'locationJSON) ->('stemmedMerchantName, 'lat, 'lng, 'merchantGeosector)) {
        in: (String, String) =>
            val (merchantName, locationJSON) = in
            val dealLocations = DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, new TypeReference[util.List[DealLocation]] {})
            val dealLocation = dealLocations.head
            val stemmedMerchantName = StemAndMetaphoneEmployer.getStemmed(merchantName)
            val geohash = GeoHash.withBitPrecision(dealLocation.latitude, dealLocation.longitude, PlaceCorrelationSectorSize)
            (stemmedMerchantName, dealLocation.latitude, dealLocation.longitude, geohash.longValue())
    }.discard('locationJSON)

    val newCheckins = correlationCheckinsFromCassandra(checkins)
    val dealVenues = correlatedPlaces(newCheckins)
            .joinWithTiny('geosector -> 'merchantGeosector, deals).groupBy('geosector) {
        _.sortWithTake(('stemmedVenName -> 'stemmedMerchantName) -> 'singleVenue, 1) {
            (a: (String, String), b: (String, String)) => Levenshtein.compareInt(a._1, a._2) < Levenshtein.compareInt(b._1, b._2)
        }.head('goldenId, 'merchantName, 'dealId)
    }
    dealVenues.write(SequenceFile(dealsOutput, ('goldenId, 'merchantName, 'dealId)))
}

object DealAnalysis {
    val DealObjectMapper = new ObjectMapper
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
