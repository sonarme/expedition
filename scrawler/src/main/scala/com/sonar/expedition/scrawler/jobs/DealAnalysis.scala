package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.`type`.TypeReference
import java.util
import com.sonar.expedition.scrawler.util.{Levenshtein, CommonFunctions, StemAndMetaphoneEmployer}
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.pipes.PlacesCorrelation.PlaceCorrelationSectorSize
import DealAnalysis._
import reflect.BeanProperty
import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import com.sonar.expedition.scrawler.pipes.{CheckinGrouperFunction, PlacesCorrelation}
import com.sonar.expedition.scrawler.jobs.DealLocation
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.Tsv
import com.sonar.scalding.cassandra.NarrowRowScheme
import cascading.tuple.Fields

class DealAnalysis(args: Args) extends Job(args) with PlacesCorrelation with CheckinGrouperFunction {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val dealsInput = args("dealsInput")
    val dealsOutput = args("dealsOutput")

    val deals = Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON)).map(('merchantName, 'locationJSON) ->('stemmedMerchantName, 'lat, 'lng, 'merchantGeosector)) {
        in: (String, String) =>
            val (merchantName, locationJSON) = in
            val dealLocations = DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, new TypeReference[util.List[DealLocation]] {})
            val dealLocation = dealLocations.head
            val stemmedMerchantName = StemAndMetaphoneEmployer.getStemmed(merchantName)
            val geohash = GeoHash.withBitPrecision(dealLocation.latitude, dealLocation.longitude, PlaceCorrelationSectorSize)
            (stemmedMerchantName, dealLocation.latitude, dealLocation.longitude, geohash.longValue())
    }.discard('locationJSON)


    val checkins = CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "Checkin",
        scheme = NarrowRowScheme(keyField = 'serviceCheckinIdBuffer,
            nameFields = ('userProfileIdBuffer, 'serTypeBuffer, 'serProfileIDBuffer, 'serCheckinIDBuffer,
                    'venNameBuffer, 'venAddressBuffer, 'venIdBuffer, 'chknTimeBuffer,
                    'ghashBuffer, 'latBuffer, 'lngBuffer, 'msgBuffer),
            columnNames = List("userProfileId", "serviceType", "serviceProfileId",
                "serviceCheckinId", "venueName", "venueAddress",
                "venueId", "checkinTime", "geohash", "latitude",
                "longitude", "message"))
    ).flatMap(('serviceCheckinIdBuffer, 'userProfileIdBuffer, 'serTypeBuffer, 'serProfileIDBuffer, 'serCheckinIDBuffer,
            'venNameBuffer, 'venAddressBuffer, 'venIdBuffer, 'chknTimeBuffer,
            'ghashBuffer, 'latBuffer, 'lngBuffer, 'msgBuffer) ->('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID,
            'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer) => {
            val rowKeyDes = StringSerializer.get().fromByteBuffer(in._1)
            val keyId = Option(in._2).map(StringSerializer.get().fromByteBuffer).getOrElse("missingKeyId")
            val serType = Option(in._3).map(StringSerializer.get().fromByteBuffer).orNull
            val serProfileID = Option(in._4).map(StringSerializer.get().fromByteBuffer).orNull
            val serCheckinID = Option(in._5).map(StringSerializer.get().fromByteBuffer).orNull
            val venName = Option(in._6).map(StringSerializer.get().fromByteBuffer).orNull
            val venAddress = Option(in._7).map(StringSerializer.get().fromByteBuffer).orNull
            val venId = Option(in._8).map(StringSerializer.get().fromByteBuffer).orNull
            val chknTime = Option(in._9).map(DateSerializer.get().fromByteBuffer).getOrElse(RichDate(0L))
            val ghash = Option(in._10).map(LongSerializer.get().fromByteBuffer).orNull
            val lat: Double = Option(in._11).map(DoubleSerializer.get().fromByteBuffer).orNull
            val lng: Double = Option(in._12).map(DoubleSerializer.get().fromByteBuffer).orNull
            val msg = Option(in._13).map(StringSerializer.get().fromByteBuffer).orNull
            // only checkins with venues
            if (CommonFunctions.isNullOrEmpty(venId))
                None
            else
                Some((rowKeyDes, keyId, serType, serProfileID, serCheckinID,
                        venName, venAddress, venId, chknTime, ghash, lat, lng, msg))
        }
    }

    val levenshtein = new Levenshtein()

    val newCheckins = correlationCheckinsFromCassandra(checkins)
    val dealVenues = correlatedPlaces(newCheckins)
            .joinWithTiny('geosector -> 'merchantGeosector, deals).groupBy('geosector) {
        _.sortWithTake(('stemmedVenName -> 'stemmedMerchantName) -> 'singleVenue, 1) {
            (a: (String, String), b: (String, String)) => levenshtein.compareInt(a._1, a._2) < levenshtein.compareInt(b._1, b._2)
        }.head('*)
    }
    dealVenues.write(SequenceFile(dealsOutput, '*))
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
