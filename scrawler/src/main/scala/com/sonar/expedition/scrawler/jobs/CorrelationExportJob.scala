package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.dossier.dao.cassandra.ServiceProfileLinkSerializer
import com.sonar.expedition.scrawler.util.Tuples

class CorrelationExportJob(args: Args) extends Job(args) with CheckinSource {
    val rpcHostArg = args("rpcHost")
    val output = args("correlationOut")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        additionalConfig = ppmap(args),
        keyspaceName = "dossier",
        columnFamilyName = "Correlation",
        scheme = WideRowScheme(keyField = 'correlationIdB,
            nameValueFields = ('serviceProfileLinkB, 'someBuffer))
    ).mapTo(('correlationIdB, 'serviceProfileLinkB) ->('correlationId, 'correlationSPL)) {
        in: (ByteBuffer, ByteBuffer) =>
            val (correlationIdB, serviceProfileLinkB) = in
            val correlationId = StringSerializer.get().fromByteBuffer(correlationIdB)
            val spl = ServiceProfileLinkSerializer.fromByteBuffer(serviceProfileLinkB)
            (correlationId, spl)
    }.write(SequenceFile(output, Tuples.Correlation))
    //         .limit(180000).write(SequenceFile(output + "_small", Tuples.Correlation))
}
