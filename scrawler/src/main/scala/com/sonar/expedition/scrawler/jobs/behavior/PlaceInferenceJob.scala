package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.dossier.dto.CheckinDTO
import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.util.{LocationClusterer, Tuples}
import com.sonar.dossier.Normalizers
import com.sonar.expedition.scrawler.checkins.CheckinInference
import com.sonar.expedition.scrawler.util.CommonFunctions._
import org.joda.time.{LocalTime, LocalDateTime}
import de.fuberlin.wiwiss.silk.plugins.metric.GeographicDistanceMetric

class PlaceInferenceJob(args: Args) extends Job(args) with Normalizers with CheckinInference {
    val argsCheckin = args("checkinsIn")
    val argsVenues = args("venuesIn")
    val argsStats = args("statsOut")
    val argsPlaceInference = args("placeInferenceOut")
    val segments = Seq(0 -> 7, 6 -> 11, 11 -> 14, 13 -> 17, 16 -> 20, 19 -> 0) map {
        case (fromHr, toHr) => Segment(from = new LocalTime(fromHr, 0, 0), to = new LocalTime(toHr, 0, 0), name = toHr)
    }
    val checkinsPipe = SequenceFile(argsCheckin, Tuples.CheckinIdDTO).read.flatMap(('checkinDto) ->('goldenId, 'canonicalVenueId, 'timeSegment)) {
        dto: CheckinDTO =>
            val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
            val weekDay = isWeekDay(ldt)

            createSegments(ldt.toLocalTime, segments) map {
                // TODO: pull in correlation
                segment => (dto.profileId, dto.serviceVenue.canonicalId, (weekDay, segment.name))
            }
    }

    val venueUserPopularity = checkinsPipe.groupBy('goldenId, 'canonicalVenueId, 'timeSegment) {
        _.size('venueUserPopularity)
    }
    val venuePopularity = venueUserPopularity.groupBy('canonicalVenueId, 'timeSegment) {
        _.sum('venueUserPopularity -> 'venuePopularity)
    }

    val clusterAveragesPipe = checkinsPipe.groupBy('goldenId, 'timeSegment) {
        _.mapList(('checkinDto) -> ('clusterAverages)) {
            checkins: List[CheckinDTO] =>
                val checkinMap = checkins.map(c => (c.latitude, c.longitude) -> c).toMap
                val clusters = LocationClusterer.cluster(checkinMap.keySet)
                clusters.flatMap {
                    cluster =>
                        val dsValues = LocationClusterer.datasetValues(cluster)
                        val clusterAverage = LocationClusterer.average(dsValues)
                        dsValues.map(checkinMap).map(_ -> clusterAverage)
                }.toSeq
        }
    }.flatMap('clusterAverages ->('checkinId, 'location))(identity[Seq[(CheckinDTO, (Double, Double))]]).discard('clusterAverages)

    val withUserPopularity = matchGeo(('goldenId, 'canonicalVenueId, 'location, 'timeSegment, 'checkinId, 'location), venueUserPopularity, clusterAveragesPipe).rename('distance -> 'score)
    val withVenuePopularity = matchGeo(('goldenId, 'canonicalVenueId, 'location, 'timeSegment, 'checkinId, 'location), venuePopularity, clusterAveragesPipe).rename('distance -> 'score)
    (withUserPopularity ++ withVenuePopularity).groupBy('goldenId, 'checkinId, 'timeSegment) {
        _.sortBy('score -> 'topScores, 3)
    } //.flatten....
    clusterAveragesPipe.write(SequenceFile(argsPlaceInference, Tuples.PlaceInference))


}
