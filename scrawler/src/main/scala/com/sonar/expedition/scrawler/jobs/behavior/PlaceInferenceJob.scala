package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.dossier.dto.{ServiceType, GeodataDTO, CheckinDTO}
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
    val checkinsPipe = SequenceFile(argsCheckin, Tuples.CheckinIdDTO).read.flatMapTo(('checkinDto) ->('checkinId, 'userGoldenId, 'serviceType, 'canonicalVenueId, 'location, 'timeSegment)) {
        dto: CheckinDTO =>
            val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
            val weekDay = isWeekDay(ldt)

            createSegments(ldt.toLocalTime, segments) map {
                // TODO: pull in correlation
                segment => (dto.canonicalId, dto.profileId, dto.serviceType, if (dto.venueId == null || dto.venueId.isEmpty) null else dto.serviceVenue.canonicalId, dto.serviceVenue.location.geodata, (weekDay, segment.name))
            }
    }

    val venueUserPopularity = checkinsPipe.filter('serviceType, 'canonicalVenueId) {
        in: (ServiceType, String) =>
            val (serviceType, canonicalVenueId) = in
            canonicalVenueId != null && serviceType == ServiceType.foursquare
    }.groupBy('userGoldenId, 'canonicalVenueId, 'location, 'timeSegment) {
        _.size('venueUserPopularity)
    }
    val venuePopularity = venueUserPopularity.groupBy('canonicalVenueId, 'location, 'timeSegment) {
        _.sum('venueUserPopularity -> 'venuePopularity)
    }

    val clusterAveragesPipe = checkinsPipe.groupBy('userGoldenId, 'timeSegment) {
        _.mapList(('checkinId, 'location) -> ('clusterAverages)) {
            checkins: List[(String, GeodataDTO)] =>
                val checkinMap = checkins.map {
                    case (checkinId, geodata) =>
                        val GeodataDTO(lat, lng) = geodata
                        (lat, lng) -> checkinId
                }.toMap
                val clusters = LocationClusterer.cluster(checkinMap.keySet)
                clusters.flatMap {
                    cluster =>
                        val dsValues = LocationClusterer.datasetValues(cluster)
                        val (avgLat, avgLng) = LocationClusterer.average(dsValues)
                        val geodata = GeodataDTO(avgLat, avgLng)
                        dsValues.map(checkinMap).map(_ -> geodata)
                }.toSeq
        }
    }.flatten[(String, GeodataDTO)]('clusterAverages ->('checkinId, 'location)).discard('clusterAverages)

    val locations = clusterAveragesPipe.unique('location)
    val withUserPopularity = matchGeo((venueUserPopularity, 'venueId, 'userGoldenId, 'location), (clusterAveragesPipe, 'checkinId, 'userGoldenId, 'location), 'score).map('score -> 'score) {
        distance: Double => distance / 10.0
    }
    val withVenuePopularity = matchGeo((venuePopularity.map(() -> 'blockId) {
        _: Unit => ""
    }, 'venueId, 'blockId, 'location), (clusterAveragesPipe.map(() -> 'blockId) {
        _: Unit => ""
    }, 'checkinId, 'blockId, 'location), 'score)
    val topScores = (withUserPopularity.project('blockId, 'idSmaller, 'idLarger, 'distance) ++ withVenuePopularity).groupBy('userGoldenId, 'checkinId, 'timeSegment) {
        _.sortWithTake(('venueId, 'score) -> 'topScores, 3) {
            (left: (String, Double), right: (String, Double)) => left._2 > right._2
        }
    }.flatten[(String, Double)]('venueId, 'score)
    topScores.write(SequenceFile(argsPlaceInference, Tuples.PlaceInference))


}
