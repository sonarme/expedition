package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.dossier.dto._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.{LocationClusterer, Tuples}
import com.sonar.dossier.{dto, Normalizers}
import com.sonar.expedition.scrawler.checkins.CheckinInference
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.util.CommonFunctions.Segment
import com.twitter.scalding.IterableSource
import com.sonar.dossier.dto.GeodataDTO
import org.scala_tools.time.Imports._
import cascading.tuple.Fields

class PlaceInferenceJob(args: Args) extends Job(args) with Normalizers with CheckinInference {
    val segments = Seq(0 -> 7, 6 -> 11, 11 -> 14, 13 -> 17, 16 -> 20, 19 -> 0) map {
        case (fromHr, toHr) => Segment(from = new LocalTime(fromHr, 0, 0), to = new LocalTime(toHr, 0, 0), name = toHr)
    }
    //val checkinSource = SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO)
    val checkinSource = IterableSource(Seq(
        ("test1", dto.CheckinDTO(ServiceType.foursquare,
            "123",
            GeodataDTO(40.0, -74.0),
            DateTime.now,
            "ben123",

            Some(ServiceVenueDTO(ServiceType.foursquare, "gg", "G&G", location = LocationDTO(GeodataDTO(40.0, -74.0), "x")))
        ))
    ), Tuples.CheckinIdDTO)

    //val placeInferenceOut = SequenceFile(args("placeInferenceOut"), Tuples.PlaceInference)
    val placeInferenceOut = NullSource
    val checkinsPipe = checkinSource.read.flatMapTo(('checkinDto) ->('checkinId, 'userGoldenId, 'serviceType, 'canonicalVenueId, 'location, 'timeSegment)) {
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
    }.rename('location, 'venueLocation).groupBy('userGoldenId, 'canonicalVenueId, 'venueLocation, 'timeSegment) {
        _.size('venueUserPopularity)
    }
    val venuePopularity = venueUserPopularity.groupBy('canonicalVenueId, 'venueLocation, 'timeSegment) {
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
    }.flatten[(String, GeodataDTO)]('clusterAverages ->('checkinId, 'location)).discard('clusterAverages, 'timeSegment)

    val metaFields: Fields = ('canonicalVenueId, 'checkinId, 'userGoldenId, 'timeSegment)
    val withUserPopularity = matchGeo((venueUserPopularity, 'userGoldenId, 'venueLocation), (clusterAveragesPipe, 'userGoldenId, 'location), 'score, metaFields).map('score -> 'score) {
        distance: Double => distance / 10.0
    }
    val withVenuePopularity = matchGeo((venuePopularity, Fields.NONE, 'venueLocation), (clusterAveragesPipe, Fields.NONE, 'location), 'score, metaFields)
    val topScores = (withUserPopularity.project('userGoldenId, 'checkinId, 'timeSegment, 'canonicalVenueId, 'score) ++ withVenuePopularity.project('userGoldenId, 'checkinId, 'timeSegment, 'canonicalVenueId, 'score)).groupBy('userGoldenId, 'checkinId, 'timeSegment) {
        _.sortWithTake(('canonicalVenueId, 'score) -> 'topScores, 3) {
            (left: (String, Double), right: (String, Double)) => left._2 > right._2
        }
    }.flatten[(String, Double)]('topScores ->('canonicalVenueId, 'score))
    topScores.write(placeInferenceOut)


}
