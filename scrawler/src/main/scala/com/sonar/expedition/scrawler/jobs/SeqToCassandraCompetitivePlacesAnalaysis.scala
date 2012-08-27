package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.dossier.dao.cassandra.{CassandraObjectMapper, CompetitiveVenueColumn, JSONSerializer, CompetitiveVenueColumnSerializer}
import com.sonar.dossier.dto.CompetitiveVenue
import com.sonar.dossier.dao.cassandra.CompetitiveVenueColumn
import com.sonar.scalding.cassandra.WideRowScheme
import com.twitter.scalding.SequenceFile
import com.sonar.scalding.cassandra.CassandraSource
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import me.prettyprint.cassandra.serializers._
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import com.fasterxml.jackson.databind.{DeserializationConfig, SerializationConfig, PropertyNamingStrategy, ObjectMapper}
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper

class SeqToCassandraCompetitivePlacesAnalaysis(args: Args) extends Job(args) {

    //184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputCompetitiveAnalysis = args.getOrElse("sequenceInputCompetitiveAnalysis", "s3n://scrawler/competitiveAnalysisOutput")

    val seqCompetitiveAnalysis = SequenceFile(sequenceInputCompetitiveAnalysis, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6) ->('venName, 'venuetype, 'goldenId, 'venName2, 'venuetype2, 'goldenId2, 'jaccardSimilarity)) {
        in: (String, String, String, String, String, String, Double) =>
            val (venueFrom, venueTypeFrom, goldenIdFrom, venueNameTo, venueTypeTo, goldenIdTo, similarityIndex) = in
            (venueFrom, venueTypeFrom, goldenIdFrom, venueNameTo, venueTypeTo, goldenIdTo, similarityIndex)
    }.map(('venName, 'venuetype, 'goldenId, 'venName2, 'venuetype2, 'goldenId2, 'jaccardSimilarity) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, String, String, String, String, Double) =>
            val (venueFrom, venueTypeFrom, goldenIdFrom, venueNameTo, venueTypeTo, goldenIdTo, similarityIndex) = in

            val analysisType = com.sonar.dossier.dto.CompetitiveAnalysisType.competitor

            val targetVenueGoldenId = goldenIdFrom

            val column = CompetitiveVenueColumn(venueGoldenId = targetVenueGoldenId, correlation = similarityIndex)

            val dto = new CompetitiveVenue(
                analysisType = analysisType,
                venueId = goldenIdTo,
                venueName = venueNameTo,
                venueType = venueTypeTo,
                correlation = similarityIndex
            )
            val columnB = CompetitiveVenueColumnSerializer toByteBuffer (column)
            val dtoB = new JSONSerializerTwo(classOf[CompetitiveVenue]) toByteBuffer (dto)

            (targetVenueGoldenId + "_" + analysisType.name, columnB, dtoB)


    }.project('rowKey, 'columnName, 'columnValue)
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

class JSONSerializerTwo[T >: Null](clazz: Class[T]) extends AbstractSerializer[T] {

    def toByteBuffer(obj: T) =
        if (obj == null) null
        else ByteBuffer.wrap(ScrawlerObjectMapper.objectMapper.writeValueAsBytes(obj))

    def fromByteBuffer(byteBuffer: ByteBuffer) =
        if (byteBuffer == null || !byteBuffer.hasRemaining) null
        else ScrawlerObjectMapper.objectMapper.readValue(ByteBufferUtil.getArray(byteBuffer), clazz)
}

