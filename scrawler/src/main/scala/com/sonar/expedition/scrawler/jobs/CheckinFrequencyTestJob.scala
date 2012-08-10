package com.sonar.expedition.scrawler.jobs

import java.nio.ByteBuffer
import com.twitter.scalding._
import com.sonar.scalding.cassandra.{NarrowRowScheme, WideRowScheme, CassandraSource}
import me.prettyprint.cassandra.serializers.{StringSerializer, LongSerializer}
import CheckinFrequencyTestJob._
import com.sonar.dossier.ScalaGoodies._

object CheckinFrequencyTestJob {
    val MsInHour = 1000 * 60 * 60
}

// RUN: mvn clean source:jar install -pl dto -am
// on dossier master, if you don't have the serializers required in here
// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class CheckinFrequencyTestJob(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")

    CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "Checkin", // SonarUser
        scheme = NarrowRowScheme(keyField = 'checkinIdB,
            nameFields = ('venueIdB, 'checkinTimeB), columnNames = List("venueId", "checkinTime"))
    )
            .flatMap(('checkinIdB, 'venueIdB, 'checkinTimeB) ->('venueId, 'checkinTimeHour)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) => {
            // filter out checkins without venue or checkin time
            for (venueId <- Option(StringSerializer.get().fromByteBuffer(in._2)) if venueId.nonEmpty;
                 checkinTime <- optionLong(LongSerializer.get().fromByteBuffer(in._3)))
            yield (venueId, checkinTime / MsInHour * MsInHour)

        }
    }.groupBy(('venueId, 'checkinTimeHour)) {
        _.size
    }.map(('venueId, 'size) ->('metricName, 'sizeAsDouble)) {
        in: (String, Int) => (in._1 + "-checkinFrequencyPerMin", in._2.toDouble)
    }.project(('metricName, 'checkinTimeHour, 'sizeAsDouble)) // maek sure triples in the correct byte buffers are written to WideRowScheme
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueTimeseries",
            scheme = WideRowScheme(keyField = 'metricName)
        )
    )
}