package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource

class SeqToCassandra(args: Args) extends Job(args) {

    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputStaticOpt = args.optional("inputStatics")
    val sequenceInputTimeOpt = args.optional("inputTime")

    sequenceInputStaticOpt foreach {
        sequenceInputStatic =>
            val seqStatic =
                sequenceInputStatic.split(',').map {
                    file => SequenceFile(file, ('rowKey, 'columnName, 'columnValue)).read.mapTo(('rowKey, 'columnName, 'columnValue) ->('rowKey, 'columnName, 'columnValue)) {
                        in: (String, String, Double) => in
                    }
                }.reduce(_ ++ _)
            seqStatic
                    .write(
                CassandraSource(
                    rpcHost = rpcHostArg,
                    privatePublicIpMap = ppmap,
                    keyspaceName = "dossier",
                    columnFamilyName = "MetricsVenueStatic",
                    scheme = WideRowScheme(keyField = 'rowKey)
                )
            )
    }

    sequenceInputTimeOpt foreach {
        sequenceInputTime =>
            val seqTime = SequenceFile(sequenceInputTime, ('rowKey, 'columnName, 'columnValue)).read

            seqTime
                    .write(
                CassandraSource(
                    rpcHost = rpcHostArg,
                    privatePublicIpMap = ppmap,
                    keyspaceName = "dossier",
                    columnFamilyName = "MetricsVenueTimeseries",
                    scheme = WideRowScheme(keyField = 'rowKey)
                )
            )
    }

}
