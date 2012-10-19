package com.sonar.expedition.scrawler.jobs

import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import java.text.SimpleDateFormat
import com.twitter.scalding._
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.util.CommonFunctions._
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.expedition.scrawler.util.{Tuples, CommonFunctions}
import com.sonar.expedition.scrawler.pipes.{ScaldingImplicits, CheckinGrouperFunction}
import java.util.Date
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.TextLine
import com.sonar.scalding.cassandra.NarrowRowScheme

trait CheckinSource extends ScaldingImplicits with CheckinGrouperFunction {
    // TODO: bad practice to use default values
    val DefaultNoDate = new Date(0L)
    val NoneValue = "none"
    val CheckinTuple = ('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID,
            'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg, 'dayOfYear, 'dayOfWeek, 'hour, 'keyid)

    val dealMatchGeosectorSize = 6

    def dealMatchGeosector(lat: Double, lng: Double) = GeoHash.withCharacterPrecision(lat, lng, dealMatchGeosectorSize).longValue()

    def dealMatchGeosectorsAdjacent(lat: Double, lng: Double) = {
        val geohash = GeoHash.withCharacterPrecision(lat, lng, dealMatchGeosectorSize)
        (geohash :: geohash.getAdjacent.toList).map(_.longValue())
    }

    def checkinSource(args: Args, venuesOnly: Boolean, withVenueGoldenId: Boolean) = {
        val rpcHostArg = args.optional("rpcHost")
        val checkinsInputArg = args.optional("checkinsInput")
        val placeClassification = args.optional("placeClassification")
        val checkins: RichPipe = checkinsInputArg match {
            case Some(checkinsInput) =>
                SequenceFile(checkinsInput, CheckinTuple).read

            case None =>
                CassandraSource(
                    rpcHost = rpcHostArg.get,
                    additionalConfig = ppmap(args),
                    keyspaceName = "dossier",
                    columnFamilyName = "Checkin",
                    scheme = NarrowRowScheme(keyField = 'rowKeyBuffer,
                        valueFields = ('serviceTypeB, 'serviceProfileIdB, 'serviceCheckinIdB,
                                'venueNameB, 'venueAddressB, 'venueIdB, 'checkinTimeB,
                                'latitudeB, 'longitudeB, 'messageB),
                        columnNames = List("serviceType", "serviceProfileId",
                            "serviceCheckinId", "venueName", "venueAddress",
                            "venueId", "checkinTime", "latitude",
                            "longitude", "message"))
                ).flatMapTo(('serviceTypeB, 'serviceProfileIdB, 'serviceCheckinIdB,
                        'venueNameB, 'venueAddressB, 'venueIdB, 'checkinTimeB,
                        'latitudeB, 'longitudeB, 'messageB) -> Tuples.Checkin) {
                    in: (ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                            ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer) => {
                        val (serviceTypeB, serviceProfileIdB, serviceCheckinIdB, venueNameB,
                        venueAddressB, venueIdB, checkinTimeB, latitudeB, longitudeB, messageB) = in

                        val serviceType = Option(serviceTypeB).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val serviceProfileId = Option(serviceProfileIdB).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val serCheckinID = Option(serviceCheckinIdB).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val venName = Option(venueNameB).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val venAddress = Option(venueAddressB).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val venId = Option(venueIdB).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val checkinTime = Option(checkinTimeB).map(DateSerializer.get().fromByteBuffer).getOrElse(DefaultNoDate)
                        val latOption = Option(latitudeB).map(DoubleSerializer.get().fromByteBuffer)
                        val lngOption = Option(longitudeB).map(DoubleSerializer.get().fromByteBuffer)
                        val msg = Option(messageB).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        // only checkins with venues
                        if (latOption.isEmpty || lngOption.isEmpty || venuesOnly && CommonFunctions.isNullOrEmpty(venId))
                            None
                        else {
                            val Some(lat) = latOption
                            val Some(lng) = lngOption
                            Some((serviceType, serviceProfileId, serCheckinID, venId, venName, venAddress, checkinTime, lat, lng, msg))
                        }
                    }
                }
        }

        if (withVenueGoldenId) {
            val places = SequenceFile(placeClassification.get, PlaceClassification.PlaceClassificationOutputTuple).read
            (checkins, withGoldenIdFromPlaces(places, checkins))

        }
        else (checkins, null)
    }

    def withGoldenIdFromPlaces(places: RichPipe, newCheckins: RichPipe): RichPipe = {
        val checkinsWithVenue = newCheckins.filter('venId) {
            venId: String => !CommonFunctions.isNullOrEmpty(venId)
        }
        places.project('venueId, 'goldenId).joinWithLarger('venueId -> 'venId, checkinsWithVenue)
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId, 'msg)
    }
}
