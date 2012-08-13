package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction
import com.sonar.dossier.dto.CompetitiveVenue
import com.sonar.dossier.dao.cassandra.{JSONSerializer, CompetitiveVenueColumn, CompetitiveVenueColumnSerializer}
import com.sonar.dossier.dto.CompetitiveVenueAnalysisType

class CompetitorAnalysisForPlaces(args: Args) extends Job(args) {

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

            var analysisType = CompetitiveVenueAnalysisType.competitor

            val column = CompetitiveVenueColumn(venueGoldenId = venueGoldenId, frequency = frequency)
            val dto = new CompetitiveVenue(
                dto.analysisType = analysisType,
                venueId = "id_" + venueName,
                venueName = venueName,
                venueType = "undefined",
                dto.frequency = frequency
            )


            val columnB = CompetitiveVenueColumnSerializer toByteBuffer (column)
            val dtoB = new JSONSerializer(classOf[CompetitiveVenue]) toByteBuffer (dto)

            (venueGoldenId + "-" + analysisType.name, column, dto.venueName)

        //(goldenVenueId, columnB, dtoB)
    }.project(('rowKey, 'columnName, 'columnValue))
            .write(TextLine("/tmp/companalyse"))
}

