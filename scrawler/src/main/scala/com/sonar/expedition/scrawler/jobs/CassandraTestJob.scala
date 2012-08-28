package com.sonar.expedition.scrawler.jobs

import java.nio.ByteBuffer
import com.twitter.scalding.{Job, Tsv, RichPipe, Args}
import com.sonar.dossier.dao.cassandra.ServiceProfileLinkSerializer
import com.sonar.scalding.cassandra.{NarrowRowScheme, WideRowScheme, CassandraSource}
import me.prettyprint.cassandra.serializers.StringSerializer

// RUN: mvn clean source:jar install -pl dto -am
// on dossier master, if you don't have the serializers required in here
// SPL means ServiceProfileLink
// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class CassandraTestJob(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "SonarUser",
        scheme = WideRowScheme(keyField = 'sonarUserIdB,
            nameField = ('columnNameB, 'columnValueB))
    )
            .mapTo(('sonarUserIdB, 'columnNameB, 'columnValueB) -> ('sonarId)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) => {
            val sid = StringSerializer.get().fromByteBuffer(in._1)
            (sid)
        }
    }.project('sonarId).groupBy('sonarId) {
        _.size
    }
            .write(Tsv("testout") /*
       CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier_ben",
            columnFamilyName = "BenTest",
            scheme = NarrowRowScheme(keyField = 'spl, nameFields = 'size, columnNames = List("count"))
        )*/
    )
}
