package com.sonar.expedition.scrawler.jobs

import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import java.text.SimpleDateFormat
import com.twitter.scalding.{RichPipe, Args, RichDate, TextLine}
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.util.CommonFunctions._
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.expedition.scrawler.util.CommonFunctions
import com.sonar.expedition.scrawler.pipes.{ScaldingImplicits, CheckinGrouperFunction}
import java.util.Date

trait CheckinSource extends ScaldingImplicits with CheckinGrouperFunction {
    // TODO: bad practice to use default values
    val DefaultNoDate = new Date(0L)
    val NoneValue = "none"

    def checkinSource(args: Args, withVenuesOnly: Boolean): RichPipe = {
        val rpcHostArg = args.optional("rpcHost")
        val ppmap = args.getOrElse("ppmap", "")
        val checkinsInputArg = args.optional("checkinsInput")
        checkinsInputArg match {
            case Some(checkinsInput) =>
                checkinsWithMessage(TextLine(checkinsInput))

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
                        if (withVenuesOnly && CommonFunctions.isNullOrEmpty(venId))
                            None
                        else
                            Some((rowKeyDes, keyId, serType, serProfileID, serCheckinID,
                                    venName, venAddress, serType + ":" + venId, chknTime, ghash, lat, lng, msg))
                    }
                }
        }
    }

}