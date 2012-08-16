package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import com.twitter.scalding.TextLine
import cascading.tuple.Fields

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222

class SeqToCassandra(args: Args) extends Job(args)  {

    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputStatic = args("sequenceInputStatic")
    val sequenceInputTime = args("sequenceInputTime")

    val seqStatic = SequenceFile(sequenceInputStatic, Fields.ALL).read.mapTo((0, 1, 2) ->('rowKey, 'columnName, 'columnValue)) {
        fields: (String, String, Double) => fields
    }

    val staticCassandra = seqStatic
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueStatic",
            scheme = WideRowScheme(keyField = 'rowKey)
        )
    )

    val seqTime = SequenceFile(sequenceInputTime, Fields.ALL).read.mapTo((0, 1, 2) ->('rowKey, 'columnName, 'columnValue)) {
        fields: (String, Long, Double) => fields
    }

    val timeSeriesCassandra = seqTime
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
