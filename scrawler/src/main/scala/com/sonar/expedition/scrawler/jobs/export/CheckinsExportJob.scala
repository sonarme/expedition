package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding._
import cascading.tuple.{Tuple, Fields}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import scala.None
import com.sonar.expedition.scrawler.util.Tuples
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers._
import com.sonar.dossier.dto.{ServiceType, CheckinDTO}
import me.prettyprint.hom.converters.JodaTimeHectorConverter
import scala.Predef._
import com.twitter.scalding.SequenceFile
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.expedition.scrawler.jobs.{CheckinSource, DefaultJob}

class CheckinsExportJob(args: Args) extends DefaultJob(args) with CheckinSource {

    val checkinsOutput = args("checkinsOut")
    val export = args.optional("export").map(_.toBoolean).getOrElse(true)

    class TupleWrapper(val tuple: Tuple) {
        def apply(idx: Int) = tuple.getObject(idx).asInstanceOf[ByteBuffer]
    }

    if (export) {
        val checkins = CassandraSource(
            rpcHost = args("rpcHost"),
            additionalConfig = ppmap(args),
            keyspaceName = "dossier",
            columnFamilyName = "Checkin",
            scheme = NarrowRowScheme(keyField = 'idB,
                valueFields = ('viewingUserSonarIdB, 'checkinTimeB, 'serviceTypeB, 'serviceProfileIdB, 'latitudeB, 'longitudeB, 'venueNameB, 'venueAddressB, 'venueSiteUrlB, 'venueIdB, 'messageB, 'serviceCheckinIdB, 'notPublicB, 'clientIdB, 'rawB, 'horizontalAccuracyB, 'verticalAccuracyB, 'batteryLevelB, 'courseB, 'speedB, 'calculatedSpeedB, 'ipB),
                columnNames = List("viewingUserSonarId", "checkinTime", "serviceType", "serviceProfileId", "latitude", "longitude", "venueName", "venueAddress", "venueSiteUrl", "venueId", "message", "serviceCheckinId", "notPublic", "clientId", "raw", "horizontalAccuracy", "verticalAccuracy", "batteryLevel", "course", "speed", "calculatedSpeed", "ipB"))
        ).flatMapTo(Fields.ALL -> (Tuples.CheckinIdDTO append('hasIp, 'serviceType))) {
            in: Tuple => {
                val tuple = new TupleWrapper(in)
                val latitude = tuple(5)
                val longitude = tuple(6)
                // not sure why this happens, but it does
                if (latitude == null || longitude == null) None
                else {
                    val checkin = new CheckinDTO
                    checkin.id = StringSerializer.get.fromByteBuffer(tuple(0))
                    checkin.viewingUserSonarId = StringSerializer.get.fromByteBuffer(tuple(1))
                    val checkinTimeBA = BytesArraySerializer.get.fromByteBuffer(tuple(2))
                    // not sure why this happens, but it does
                    checkin.checkinTime = if (checkinTimeBA == null) null else (new JodaTimeHectorConverter) convertCassTypeToObjType(null, checkinTimeBA)
                    checkin.serviceType = ServiceType.valueOf(checkin.id.split(':')(0))
                    checkin.serviceProfileId = StringSerializer.get.fromByteBuffer(tuple(4))
                    checkin.latitude = DoubleSerializer.get.fromByteBuffer(latitude)
                    checkin.longitude = DoubleSerializer.get.fromByteBuffer(longitude)
                    checkin.venueName = StringSerializer.get.fromByteBuffer(tuple(7))
                    checkin.venueAddress = StringSerializer.get.fromByteBuffer(tuple(8))
                    checkin.venueSiteUrl = StringSerializer.get.fromByteBuffer(tuple(9))
                    checkin.venueId = StringSerializer.get.fromByteBuffer(tuple(10))
                    checkin.message = StringSerializer.get.fromByteBuffer(tuple(11))
                    checkin.serviceCheckinId = StringSerializer.get.fromByteBuffer(tuple(12))
                    checkin.notPublic = BooleanSerializer.get.fromByteBuffer(tuple(13))
                    checkin.clientId = StringSerializer.get.fromByteBuffer(tuple(14))
                    checkin.raw = StringSerializer.get.fromByteBuffer(tuple(15))
                    checkin.horizontalAccuracy = DoubleSerializer.get.fromByteBuffer(tuple(16))
                    checkin.verticalAccuracy = DoubleSerializer.get.fromByteBuffer(tuple(17))
                    checkin.batteryLevel = DoubleSerializer.get.fromByteBuffer(tuple(18))
                    checkin.course = DoubleSerializer.get.fromByteBuffer(tuple(19))
                    checkin.speed = DoubleSerializer.get.fromByteBuffer(tuple(20))
                    checkin.calculatedSpeed = DoubleSerializer.get.fromByteBuffer(tuple(21))
                    checkin.ip = StringSerializer.get.fromByteBuffer(tuple(22))
                    Some((checkin.id, checkin, checkin.ip != null, checkin.serviceType))
                }
            }
        }

        checkins.write(SequenceFile(checkinsOutput, Tuples.CheckinIdDTO))
    } else {
        val checkins = SequenceFile(checkinsOutput, Tuples.CheckinIdDTO).read
        val reducedCheckins = checkins.discard('checkinDto)
        // write stats
        val serviceTypeStats = reducedCheckins.groupBy('serviceType) {
            _.size
        }.map(('serviceType) -> 'statName) {
            serviceType: ServiceType => "num_" + serviceType.name()
        }.project('statName, 'size)
        val hasIpStats = reducedCheckins.groupBy('hasIp) {
            _.size
        }.map('hasIp -> 'statName) {
            hasIp: Boolean => "num_hasIp_" + hasIp
        }.project('statName, 'size)

        (serviceTypeStats ++ hasIpStats).write(Tsv(checkinsOutput + "_stats", ('statName, 'size)))
    }
}
