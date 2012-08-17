package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Job, Args}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import java.nio.ByteBuffer
import com.sonar.dossier.dao.cassandra.{CompetitiveVenueColumn, JSONSerializer, CompetitiveVenueColumnSerializer}
import com.sonar.dossier.dto.CompetitiveVenue

class SeqToCassandraCompetitivePlacesAnalaysis(args: Args) extends Job(args) {


    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputCompetitiveAnalysis = args.getOrElse("sequenceInputCompetitiveAnalysis", "s3n://scrawler/competitiveAnalysisOutput")

    val seqCompetitiveAnalysis = SequenceFile(sequenceInputCompetitiveAnalysis, Fields.ALL).read.mapTo((0, 1, 2, 3, 4) ->('venName, 'goldenId, 'venName2, 'goldenId2, 'jaccardSimilarity)) {
        in: (String, String, String, String, Double) =>
            val (venueFrom, goldenIdFrom, venueNameTo, goldenIdTo, similarityIndex) = in
            (venueFrom, goldenIdFrom, venueNameTo, goldenIdTo, similarityIndex)
    }.map(('venName, 'goldenId, 'venName2, 'goldenId2, 'jaccardSimilarity) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, String, String, Double) =>
            val (venueFrom, goldenIdFrom, venueNameTo, goldenIdTo, similarityIndex) = in

            val analysisType = com.sonar.dossier.dto.CompetitiveAnalysisType.competitor

            val targetVenueGoldenId = goldenIdFrom

            val column = CompetitiveVenueColumn(venueGoldenId = targetVenueGoldenId, correlation = similarityIndex)

            val dto = new CompetitiveVenue(
                analysisType = analysisType,
                venueId = goldenIdTo,
                venueName = venueNameTo,
                venueType = "undefined",
                correlation = similarityIndex
            )
            val columnB = CompetitiveVenueColumnSerializer toByteBuffer (column)
            val dtoB = new JSONSerializer(classOf[CompetitiveVenue]) toByteBuffer (dto)

            (targetVenueGoldenId + "_" + analysisType.name, columnB, dtoB)


    }
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueCompetitiveAnalysis",
            scheme = WideRowScheme(keyField = 'rowKey)
        ))

}
