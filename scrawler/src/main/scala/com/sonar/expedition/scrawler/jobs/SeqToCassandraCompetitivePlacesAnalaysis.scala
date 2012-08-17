package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Job, Args}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import java.nio.ByteBuffer
import com.sonar.dossier.dao.cassandra.{JSONSerializer, CompetitiveVenueColumnSerializer}
import com.sonar.dossier.dto.CompetitiveVenue

class SeqToCassandraCompetitivePlacesAnalaysis(args: Args) extends Job(args) {


    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputCompetitiveAnalysis = args("sequenceInputCompetitiveAnalysis")

    val seqCompetitiveAnalysis = SequenceFile(sequenceInputCompetitiveAnalysis, Fields.ALL).read.mapTo((0, 1, 2) ->('rowKey, 'columnName, 'columnValue)) {
        fields: (String, ByteBuffer, ByteBuffer) => fields
        val (rowKey, columnName, columnValue) = in

        val columnB = CompetitiveVenueColumnSerializer fromByteBuffer (columnName)
        val dtoB = new JSONSerializer(classOf[CompetitiveVenue]) fromByteBuffer (columnValue)

        (rowKey, columnB, dtoB)
    }

    val staticCassandra = seqCompetitiveAnalysis
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueCompetitiveAnalysis",
            scheme = WideRowScheme(keyField = 'rowKey)
        )
    )

}
