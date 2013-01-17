package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding._
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import scala.{None, Some}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Tuples}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers._
import com.sonar.dossier.dto.{ServiceType, CheckinDTO}
import com.sonar.dossier.cassandra.converters.ServiceTypeConverter
import me.prettyprint.hom.converters.JodaTimeHectorConverter
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import scala.Predef._
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.twitter.scalding.SequenceFile
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.expedition.scrawler.jobs.{CheckinSource, DefaultJob}

class CheckinsExportJob(args: Args) extends DefaultJob(args) with CheckinSource {

    val checkins = CassandraSource(
        rpcHost = args("rpcHost"),
        additionalConfig = ppmap(args) ++ Map("cassandra.range.batch.size" -> "2048"),
        keyspaceName = "dossier",
        columnFamilyName = "Checkin",
        scheme = NarrowRowScheme(keyField = 'idB,
            valueFields = ('viewingUserSonarIdB, 'checkinTimeB, 'serviceTypeB, 'serviceProfileIdB, 'latitudeB, 'longitudeB, 'venueNameB, 'venueAddressB, 'venueSiteUrlB, 'venueIdB, 'messageB, 'serviceCheckinIdB, 'notPublicB, 'clientIdB, 'rawB, 'horizontalAccuracyB, 'verticalAccuracyB, 'batteryLevelB, 'courseB, 'speedB, 'calculatedSpeedB),
            columnNames = List("viewingUserSonarId", "checkinTime", "serviceType", "serviceProfileId", "latitude", "longitude", "venueName", "venueAddress", "venueSiteUrl", "venueId", "message", "serviceCheckinId", "notPublic", "clientId", "raw", "horizontalAccuracy", "verticalAccuracy", "batteryLevel", "course", "speed", "calculatedSpeed"))
    ).flatMapTo(('idB, 'viewingUserSonarIdB, 'checkinTimeB, 'serviceTypeB, 'serviceProfileIdB, 'latitudeB, 'longitudeB, 'venueNameB, 'venueAddressB, 'venueSiteUrlB, 'venueIdB, 'messageB, 'serviceCheckinIdB, 'notPublicB, 'clientIdB, 'rawB, 'horizontalAccuracyB, 'verticalAccuracyB, 'batteryLevelB, 'courseB, 'speedB, 'calculatedSpeedB) -> (Tuples.CheckinIdDTO append ('serviceType))) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer) => {
            val (id, viewingUserSonarId, checkinTime, serviceType, serviceProfileId, latitude, longitude, venueName, venueAddress, venueSiteUrl, venueId, message, serviceCheckinId, notPublic, clientId, raw, horizontalAccuracy, verticalAccuracy, batteryLevel, course, speed, calculatedSpeed) = in
            // not sure why this happens, but it does
            if (latitude == null || longitude == null) None
            else {
                val checkin = new CheckinDTO
                checkin.id = StringSerializer.get.fromByteBuffer(id)
                checkin.viewingUserSonarId = StringSerializer.get.fromByteBuffer(viewingUserSonarId)
                val checkinTimeBA = BytesArraySerializer.get.fromByteBuffer(checkinTime)
                // not sure why this happens, but it does
                checkin.checkinTime = if (checkinTimeBA == null) null else (new JodaTimeHectorConverter) convertCassTypeToObjType(null, checkinTimeBA)
                checkin.serviceType = ServiceType.valueOf(checkin.id.split(':')(0))
                checkin.serviceProfileId = StringSerializer.get.fromByteBuffer(serviceProfileId)
                checkin.latitude = DoubleSerializer.get.fromByteBuffer(latitude)
                checkin.longitude = DoubleSerializer.get.fromByteBuffer(longitude)
                checkin.venueName = StringSerializer.get.fromByteBuffer(venueName)
                checkin.venueAddress = StringSerializer.get.fromByteBuffer(venueAddress)
                checkin.venueSiteUrl = StringSerializer.get.fromByteBuffer(venueSiteUrl)
                checkin.venueId = StringSerializer.get.fromByteBuffer(venueId)
                checkin.message = StringSerializer.get.fromByteBuffer(message)
                checkin.serviceCheckinId = StringSerializer.get.fromByteBuffer(serviceCheckinId)
                checkin.notPublic = BooleanSerializer.get.fromByteBuffer(notPublic)
                checkin.clientId = StringSerializer.get.fromByteBuffer(clientId)
                checkin.raw = StringSerializer.get.fromByteBuffer(raw)
                checkin.horizontalAccuracy = DoubleSerializer.get.fromByteBuffer(horizontalAccuracy)
                checkin.verticalAccuracy = DoubleSerializer.get.fromByteBuffer(verticalAccuracy)
                checkin.batteryLevel = DoubleSerializer.get.fromByteBuffer(batteryLevel)
                checkin.course = DoubleSerializer.get.fromByteBuffer(course)
                checkin.speed = DoubleSerializer.get.fromByteBuffer(speed)
                checkin.calculatedSpeed = DoubleSerializer.get.fromByteBuffer(calculatedSpeed)
                Some((checkin.id, checkin, checkin.serviceType))
            }
        }
    }

    val checkinsOutput = args("checkinsOut")
    checkins.write(SequenceFile(checkinsOutput, Fields.ALL))

    // write stats
    checkins.groupBy('serviceType) {
        _.size
    }.write(Tsv(checkinsOutput + "_stats", ('serviceType, 'size)))
}
