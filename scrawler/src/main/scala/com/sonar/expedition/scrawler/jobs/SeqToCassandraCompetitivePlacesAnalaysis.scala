package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Job, Args}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import java.nio.ByteBuffer
import com.sonar.dossier.dao.cassandra.{CassandraObjectMapper, CompetitiveVenueColumn, JSONSerializer, CompetitiveVenueColumnSerializer}
import com.sonar.dossier.dto.CompetitiveVenue
import me.prettyprint.cassandra.serializers.AbstractSerializer
import org.apache.cassandra.utils.ByteBufferUtil
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind._
import com.sonar.dossier.dao.cassandra.CompetitiveVenueColumn
import com.sonar.scalding.cassandra.WideRowScheme
import com.twitter.scalding.SequenceFile
import com.sonar.scalding.cassandra.CassandraSource
import org.codehaus.jackson.map.{SerializationConfig, DeserializationConfig, PropertyNamingStrategy, ObjectMapper}
import reflect.BeanProperty
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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
    val objectMapper = new CassandraObjectMapperTwo

    def toByteBuffer(obj: T) =
        if (obj == null) null
        else ByteBuffer.wrap(objectMapper.writeValueAsBytes(obj))

    def fromByteBuffer(byteBuffer: ByteBuffer) =
        if (byteBuffer == null || !byteBuffer.hasRemaining) null
        else objectMapper.readValue(ByteBufferUtil.getArray(byteBuffer), clazz)
}


class CassandraObjectMapperTwo extends ObjectMapper {
    setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
    disable(SerializationConfig.Feature.AUTO_DETECT_FIELDS)
    disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES)
    registerModule(DefaultScalaModule)
}