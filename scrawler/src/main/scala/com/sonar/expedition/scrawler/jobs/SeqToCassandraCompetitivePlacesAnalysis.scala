package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, Job, SequenceFile, Args}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.dossier.dao.cassandra.{CassandraObjectMapper, CompetitiveVenueColumn, JSONSerializer, CompetitiveVenueColumnSerializer}
import com.sonar.dossier.dto.{CompetitiveAnalysisType, CompetitiveVenue}
import com.sonar.dossier.dao.cassandra.CompetitiveVenueColumn
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import me.prettyprint.cassandra.serializers._
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import com.fasterxml.jackson.databind.{DeserializationConfig, SerializationConfig, PropertyNamingStrategy, ObjectMapper}
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper

class SeqToCassandraCompetitivePlacesAnalysis(args: Args) extends Job(args) {
    val analysisType = CompetitiveAnalysisType.competitor

    //184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputCompetitiveAnalysis = args.getOrElse("competitiveAnalysisOutput", "s3n://scrawler/competitiveAnalysisOutput")

    val seqCompetitiveAnalysis = SequenceFile(
        sequenceInputCompetitiveAnalysis,
        ('venName, 'venueTypes, 'goldenId, 'venName2, 'venueTypes2, 'goldenId2, 'jaccardSimilarity)
    )
            .read
            .mapTo(('venName, 'venueTypes, 'goldenId, 'venName2, 'venueTypes2, 'goldenId2, 'jaccardSimilarity) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, List[String], String, String, List[String], String, Double) =>
            val (_, _, goldenIdFrom, venueNameTo, venueTypesTo, goldenIdTo, similarityIndex) = in


            val targetVenueGoldenId = goldenIdFrom

            val column = CompetitiveVenueColumn(venueGoldenId = targetVenueGoldenId, correlation = similarityIndex)

            val dto = new CompetitiveVenue(
                analysisType = analysisType,
                venueId = goldenIdTo,
                venueName = venueNameTo,
                venueType = venueTypesTo.mkString(","),
                correlation = similarityIndex
            )
            val columnB = CompetitiveVenueColumnSerializer toByteBuffer (column)
            val dtoB = new JSONSerializerTwo(classOf[CompetitiveVenue]) toByteBuffer (dto)

            (targetVenueGoldenId + "_" + analysisType.name, columnB, dtoB)


    }
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

