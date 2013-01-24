package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.dossier.dao.cassandra.ServiceProfileLinkSerializer
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.jobs.{CheckinSource, DefaultJob}
import com.sonar.dossier.dto.{Priorities, Correlation, ServiceProfileLink}
import collection.JavaConversions._

class CorrelationExportJob(args: Args) extends DefaultJob(args) with CheckinSource {
    val rpcHostArg = args("rpcHost")
    val output = args("correlationOut")
    val export = args.optional("export").map(_.toBoolean).getOrElse(false)
    if (export) {
        CassandraSource(
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
    }
    else {
        SequenceFile(output, Tuples.Correlation).read.groupBy('correlationId) {
            _.mapList('correlationSPL -> 'correlation) {
                spls: List[ServiceProfileLink] =>
                    new Correlation(spls.toSet[ServiceProfileLink])
            }
        }.flatMapTo('correlation ->('userGoldenSPL, 'correlationSPL)) {
            correlation: Correlation =>
                correlation.links map {
                    case link => (correlation.goldenIdLink, link)
                }
        }.write(SequenceFile(output + "_golden", Tuples.CorrelationGolden))
    }
}
