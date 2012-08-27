package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, RichPipe, Args, TextLine}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import cascading.tuple.Fields

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222

class SeqToCassandraIncome(args: Args) extends Job(args) {

    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputIncome = args("sequenceInputIncome")

    val seqIncome = SequenceFile(sequenceInputIncome, Fields.ALL).read.mapTo((0, 1, 2) ->('rowKey, 'columnName, 'columnValue)) {
        fields: (String, String, Double) => fields
    }

    val staticCassandra = seqIncome
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
