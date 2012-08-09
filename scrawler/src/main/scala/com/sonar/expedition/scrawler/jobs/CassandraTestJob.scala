package com.sonar.expedition.scrawler.jobs

import java.nio.ByteBuffer
import com.twitter.scalding._
import com.sonar.dossier.dao.cassandra.ServiceProfileLinkSerializer
import com.sonar.scalding.cassandra.{NarrowRowScheme, WideRowScheme, CassandraSource}
import me.prettyprint.cassandra.serializers.StringSerializer

// RUN: mvn clean source:jar install -pl dto -am
// on dossier master, if you don't have the serializers required in here
// SPL means ServiceProfileLink
class CassandraTestJob(args: Args) extends Job(args) {
    CassandraSource(
        //rpcHost = "184.73.11.214",
        rpcHost = "10.4.103.222",
        //privatePublicIpMap = Map("10.4.103.222" -> "184.73.11.214", "10.96.143.88" -> "50.16.106.193"),
        keyspaceName = "dossier",
        columnFamilyName = "SonarUser", // ProfileView
        scheme = WideRowScheme(keyField = 'privacySPLB,
            nameField = ('targetSPLB, 'profileB))
    )
            .map(('privacySPLB, 'targetSPLB, 'profileB) -> ('spl)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) => {
            val spl = StringSerializer.get().fromByteBuffer(in._1) //ServiceProfileLinkSerializer.fromByteBuffer(in._2).profileId
            (spl)
        }
    }.project('spl).groupBy('spl) {
        _.size
    }
            .write(
        CassandraSource(
            //rpcHost = "184.73.11.214",
            rpcHost = "10.4.103.222",
            //privatePublicIpMap = Map("10.4.103.222" -> "184.73.11.214", "10.96.143.88" -> "50.16.106.193"),
            keyspaceName = "dossier_ben",
            columnFamilyName = "BenTest",
            scheme = NarrowRowScheme(keyField = 'spl, nameFields = 'size, columnNames = List("count"))
        )
    )
}
