package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.{JSONSerializer, CompetitiveVenueColumn, CompetitiveVenueColumnSerializer}
import com.sonar.dossier.dto.CompetitiveAnalysisType
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}

class CompetitorAnalysisForPlaces(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")

    val chkininputData = args("checkinData")
    val checkinGrouperPipe = new CheckinGrouperFunction(args)

    val chkindata = checkinGrouperPipe.unfilteredCheckinsLatLon(TextLine(chkininputData).read)


    //('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat,'lon, 'dayOfYear, 'dayOfWeek, 'hour)

    val chkindatarenamepipe = chkindata.rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour) ->
            ('keyid2, 'serType2, 'serProfileID2, 'serCheckinID2, 'venName2, 'venAddress2, 'chknTime2, 'ghash2, 'lat2, 'lng2, 'dayOfYear2, 'dayOfWeek2, 'hour2))

    val joinedcorrelation = chkindatarenamepipe.joinWithLarger('keyid2 -> 'keyid, chkindata)
            .project(('venName, 'lat, 'lng, 'venName2, 'lat2, 'lng2))
            .filter('venName, 'venName2) {
        places: (String, String) => (places._1.trim != places._2.trim && places._1.trim != "" && places._2.trim != "")
    }
            .groupBy('venName, 'venName2) {
        _.size
    }.map(('venName, 'venName2, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            var (venueGoldenId, venueName, frequency) = in

            val analysisType = com.sonar.dossier.dto.CompetitiveAnalysisType.competitor
            val targetVenueGoldenId = "id_" + venueName
            val column = CompetitiveVenueColumn(venueGoldenId = targetVenueGoldenId, frequency = frequency)
            val dto = new CompetitiveVenue(
                analysisType = analysisType,
                venueId = targetVenueGoldenId,
                venueName = venueName,
                venueType = "undefined",
                frequency = frequency
            )


            val columnB = CompetitiveVenueColumnSerializer toByteBuffer (column)
            val dtoB = new JSONSerializer(classOf[CompetitiveVenue]) toByteBuffer (dto)

            (venueGoldenId + "-" + analysisType.name, columnB, dtoB)

    }.project(('rowKey, 'columnName, 'columnValue))
            //.write(TextLine("/tmp/companalyse"))
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

