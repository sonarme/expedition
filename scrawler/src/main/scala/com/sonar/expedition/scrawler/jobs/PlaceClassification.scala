package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.pipes.{LocationBehaviourAnalysePipe, PlacesCorrelation}
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.SequenceFile
import java.text.SimpleDateFormat
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import scala.Some
import com.twitter.scalding.TextLine
import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import com.sonar.expedition.scrawler.util.CommonFunctions

import com.sonar.expedition.scrawler.pipes.JobImplicits._

class PlaceClassification(args: Args) extends Job(args) with PlacesCorrelation with CheckinSource {

    val bayestrainingmodel = args("bayestrainingmodelforlocationtype")
    val placesData = args("placesData")
    val output = args("placesOutput")

    val checkinsInputPipe = {
        val rpcHostArg = args.optional("rpcHost")
        val ppmap = args.getOrElse("ppmap", "")
        val checkinsInputArg = args.optional("checkinsInput")
        checkinsInputArg match {
            case Some(checkinsInput) =>
                def getDate(chknTime: String) = {
                    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'")
                    try {
                        val chekingTime = chknTime.substring(chknTime.lastIndexOf(":") + 1)
                        Some(simpleDateFormat.parse(chekingTime))

                    } catch {
                        case e => None
                    }
                }

                checkinsWithMessage(TextLine(checkinsInput).read)

                        .flatMapTo(
                    ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'msg)
                            ->
                            ('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg)
                ) {

                    fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>
                        val (keyid, serType, serProfileID, serCheckinID, venName, venAddress, venId, chknTime, ghash, lat, lng, dayOfYear, dayOfWeek, hour, msg) = fields

                        val gHashAsLong = Option(ghash).map(GeoHash.fromGeohashString(_).longValue()).getOrElse(0L)
                        getDate(chknTime) map {
                            checkinTime =>
                                (serType + ":" + venId, keyid, serType, hashed(serProfileID), serCheckinID, venName, venAddress, venId, checkinTime, gHashAsLong, lat.toDouble, lng.toDouble, msg)
                        }
                }


            case None =>
                CassandraSource(
                    rpcHost = rpcHostArg.get,
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
                ).flatMapTo(('serviceCheckinIdBuffer, 'userProfileIdBuffer, 'serTypeBuffer, 'serProfileIDBuffer, 'serCheckinIDBuffer,
                        'venNameBuffer, 'venAddressBuffer, 'venIdBuffer, 'chknTimeBuffer,
                        'ghashBuffer, 'latBuffer, 'lngBuffer, 'msgBuffer) ->('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID,
                        'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg)) {
                    in: (ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                            ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer) => {
                        val rowKeyDes = StringSerializer.get().fromByteBuffer(in._1)
                        val keyId = Option(in._2).map(StringSerializer.get().fromByteBuffer).getOrElse("missingKeyId")
                        val serType = Option(in._3).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val serProfileID = Option(in._4).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val serCheckinID = Option(in._5).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val venName = Option(in._6).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val venAddress = Option(in._7).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val venId = Option(in._8).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        val chknTime = Option(in._9).map(DateSerializer.get().fromByteBuffer).getOrElse(DefaultNoDate)
                        val ghash = Option(in._10).map(LongSerializer.get().fromByteBuffer).orNull
                        val lat: Double = Option(in._11).map(DoubleSerializer.get().fromByteBuffer).orNull
                        val lng: Double = Option(in._12).map(DoubleSerializer.get().fromByteBuffer).orNull
                        val msg = Option(in._13).map(StringSerializer.get().fromByteBuffer).getOrElse(NoneValue)
                        // only checkins with venues
                        if (true && CommonFunctions.isNullOrEmpty(venId))
                            None
                        else
                            Some((rowKeyDes, keyId, serType, serProfileID, serCheckinID,
                                    venName, venAddress, venId, chknTime, ghash, lat, lng, msg))
                    }
                }
        }
    }

    val placesVenueGoldenId = placeClassification(checkinsInputPipe, bayestrainingmodel, placesData)

            .write(
        Tsv(output, Fields.ALL))


}

