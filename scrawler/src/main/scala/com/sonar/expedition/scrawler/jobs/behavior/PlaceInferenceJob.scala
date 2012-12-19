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
    val segments = Seq(0 -> 7, 7 -> 11, 11 -> 14, 14 -> 16, 16 -> 20, 20 -> 0) map {
        case (fromHr, toHr) => Segment(from = new LocalTime(fromHr, 0, 0), to = new LocalTime(toHr, 0, 0), name = toHr)
    }
    //val checkinSource = SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO)
    val checkinSource = IterableSource(Seq(
        ("test1", dto.CheckinDTO(ServiceType.foursquare,
            "test1",
            GeodataDTO(40.0, -74.0),
            DateTime.now,
            "ben123",

            Some(ServiceVenueDTO(ServiceType.foursquare, "gg", "G&G", location = LocationDTO(GeodataDTO(40.0, -74.0), "x")))
        )),
        ("test1", dto.CheckinDTO(ServiceType.sonar,
            "test1",
            GeodataDTO(40.0, -74.000001),
            DateTime.now,
            "ben123",

            None
        ))
    ), Tuples.CheckinIdDTO)

    val placeInferenceOut = Tsv(args("placeInferenceOut"), Tuples.PlaceInference)

    val correlation = IterableSource(Seq(
        ("c1", ServiceType.foursquare, "ben123"),
        ("c1", ServiceType.sonar, "ben123")
    ), Tuples.Correlation)

    val segmentedCheckins = checkinSource.read.flatMapTo(('checkinDto) ->('checkinId, 'serviceProfileId, 'serviceType, 'canonicalVenueId, 'location, 'timeSegment)) {
        dto: CheckinDTO =>
            val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
            val weekDay = isWeekDay(ldt)
            // only treat foursquare venues as venues
            val canonicalVenueId = if (dto.venueId == null || dto.venueId.isEmpty || dto.serviceType != ServiceType.foursquare) null else dto.serviceVenue.canonicalId
            createSegments(ldt.toLocalTime, segments) map {
                segment => (dto.canonicalId, dto.serviceProfileId, dto.serviceType, canonicalVenueId, dto.serviceVenue.location.geodata, TimeSegment(weekDay, segment.name))
            }
    }.joinWithSmaller(('serviceType, 'serviceProfileId) ->('serviceType, 'serviceProfileId), correlation).rename('correlationId -> 'userGoldenId)

    // popularity of a venue with a particular user in a time segment
    val venueUserPopularity = segmentedCheckins.filter('canonicalVenueId) {
        canonicalVenueId: String => canonicalVenueId != null
    }.rename('location, 'venueLocation).groupBy('userGoldenId, 'canonicalVenueId, 'venueLocation, 'timeSegment) {
        _.size('venueUserPopularity)
    }

    // popularity of a venue with all users in a time segment
    val venuePopularity = venueUserPopularity.groupBy('canonicalVenueId, 'venueLocation, 'timeSegment) {
        _.sum('venueUserPopularity -> 'venuePopularity)
    }

    val clusterAveragesPipe = segmentedCheckins.groupBy('userGoldenId, 'timeSegment) {
        _.mapList(('checkinId, 'location) -> ('adjustedCheckinLocations)) {
            checkins: List[(String, GeodataDTO)] =>
                val geoToCheckinIdMap = checkins.map {
                    case (checkinId, geodata) =>
                        val GeodataDTO(lat, lng) = geodata
                        (lat, lng) -> checkinId
                }.toMap
                val checkinLocations = geoToCheckinIdMap.keySet
                val clusters = LocationClusterer.cluster(checkinLocations, 50, 1)
                val adjustedCheckinLocations = clusters.flatMap {
                    cluster =>
                        val dsValues = LocationClusterer.datasetValues(cluster)
                        val (avgLat, avgLng) = LocationClusterer.average(dsValues)
                        val geodata = GeodataDTO(avgLat, avgLng)
                        dsValues.map(geoToCheckinIdMap).map(_ -> geodata)
                }.toSeq
                println("ca:" + adjustedCheckinLocations)
                adjustedCheckinLocations
        }
    }.flatten[(String, GeodataDTO)]('adjustedCheckinLocations ->('checkinId, 'location)).discard('adjustedCheckinLocations, 'timeSegment)

    val metaFields: Fields = ('canonicalVenueId, 'checkinId, 'userGoldenId, 'timeSegment)
    val withUserPopularity = matchGeo((venueUserPopularity, 'userGoldenId, 'venueLocation, 'venueUserPopularity), (clusterAveragesPipe, 'userGoldenId, 'location), 'userScore, metaFields).rename(('userGoldenId, 'canonicalVenueId, 'checkinId, 'timeSegment) -> (('user_userGoldenId, 'user_canonicalVenueId, 'user_checkinId, 'user_timeSegment)))
    val withVenuePopularity = matchGeo((venuePopularity, Fields.NONE, 'venueLocation, 'venuePopularity), (clusterAveragesPipe, Fields.NONE, 'location), 'allScore, metaFields)
    val topScores = withVenuePopularity.leftJoinWithSmaller(('userGoldenId, 'canonicalVenueId, 'checkinId, 'timeSegment) ->('user_userGoldenId, 'user_canonicalVenueId, 'user_checkinId, 'user_timeSegment), withUserPopularity)
            // create composite score from user and allUser score
            .map(('userScore, 'allScore) -> 'score) {
        in: (java.lang.Double, java.lang.Double) =>
            println("score: " + in)
            if (in._1 == null) in._2 else (in._1 + in._2) / 2.0
    }
            .groupBy('checkinId) {
        // pick the top 3 scores for each checkin
        _.sortWithTake[cascading.tuple.Tuple](('score, 'userGoldenId, 'timeSegment, 'canonicalVenueId) -> 'topScores, 3) {
            (left: cascading.tuple.Tuple, right: cascading.tuple.Tuple) =>
                println(left + " " + right)
                left.getDouble(0) > right.getDouble(0)
        }
    }.flatten[cascading.tuple.Tuple]('topScores ->('score, 'userGoldenId, 'timeSegment, 'canonicalVenueId))
    topScores.write(placeInferenceOut)


}

case class TimeSegment(weekday: Boolean, segment: Int) extends Comparable[TimeSegment] {
    def compareTo(o: TimeSegment) = Ordering[(Boolean, Int)].compare((weekday, segment), (o.weekday, o.segment))
}
