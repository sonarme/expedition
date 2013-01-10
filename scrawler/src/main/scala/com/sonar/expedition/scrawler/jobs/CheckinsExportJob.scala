package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import scala.{None, Some}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Tuples}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers._
import com.sonar.dossier.dto.{ServiceType, CheckinDTO}
import com.sonar.dossier.cassandra.converters.ServiceTypeConverter
import me.prettyprint.hom.converters.JodaTimeHectorConverter
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.SequenceFile
import com.sonar.scalding.cassandra.NarrowRowScheme

class CheckinsExportJob(args: Args) extends Job(args) with CheckinSource {


    /**
     * By default we only set two keys:
     * io.serializations
     * cascading.tuple.element.comparator.default
     * Override this class, call base and ++ your additional
     * map to set more options
     override def config =
         super.config ++
                 Map("mapred.map.max.attempts" -> "20",
                     "mapred.reduce.max.attempts" -> "20",
                     "mapred.max.tracker.failures" -> "20")
     */

    override def config =
        super.config ++
                Map("mapred.map.tasks" -> "80")

    val checkins = CassandraSource(
        rpcHost = args("rpcHost"),
        additionalConfig = ppmap(args),
        keyspaceName = "dossier",
        columnFamilyName = "Checkin",
        scheme = NarrowRowScheme(keyField = 'idB,
            valueFields = ('viewingUserSonarIdB, 'checkinTimeB, 'serviceTypeB, 'serviceProfileIdB, 'latitudeB, 'longitudeB, 'venueNameB, 'venueAddressB, 'venueSiteUrlB, 'venueIdB, 'messageB, 'serviceCheckinIdB, 'notPublicB, 'clientIdB, 'rawB, 'horizontalAccuracyB, 'verticalAccuracyB, 'batteryLevelB, 'courseB, 'speedB, 'calculatedSpeedB),
            columnNames = List("viewingUserSonarId", "checkinTime", "serviceType", "serviceProfileId", "latitude", "longitude", "venueName", "venueAddress", "venueSiteUrl", "venueId", "message", "serviceCheckinId", "notPublic", "clientId", "raw", "horizontalAccuracy", "verticalAccuracy", "batteryLevel", "course", "speed", "calculatedSpeed"))
    ).flatMapTo(('idB, 'viewingUserSonarIdB, 'checkinTimeB, 'serviceTypeB, 'serviceProfileIdB, 'latitudeB, 'longitudeB, 'venueNameB, 'venueAddressB, 'venueSiteUrlB, 'venueIdB, 'messageB, 'serviceCheckinIdB, 'notPublicB, 'clientIdB, 'rawB, 'horizontalAccuracyB, 'verticalAccuracyB, 'batteryLevelB, 'courseB, 'speedB, 'calculatedSpeedB) -> Tuples.CheckinIdDTO) {
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
                Some((checkin.id, checkin))
            }
        }
    }

    val checkinsOutput = args("checkinsOut")
    checkins.write(SequenceFile(checkinsOutput, Fields.ALL))
    //.limit(18000).write(SequenceFile(checkinsOutput + "_small", Fields.ALL))
}
