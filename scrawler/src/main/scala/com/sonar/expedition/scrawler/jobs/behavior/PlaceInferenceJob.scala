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
import cascading.tuple.{Tuple, Fields}
import grizzled.slf4j.Logging
import com.sonar.expedition.scrawler.jobs.DefaultJob

class PlaceInferenceJob(args: Args) extends DefaultJob(args) with Normalizers with CheckinInference {
    val segments = Seq(0 -> 7, 7 -> 11, 11 -> 14, 14 -> 16, 16 -> 20, 20 -> 0) map {
        case (fromHr, toHr) => Segment(from = new LocalTime(fromHr, 0, 0), to = new LocalTime(toHr, 0, 0), name = toHr)
    }
    val checkinSource = SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO).read
    /*val checkinSource = IterableSource(Seq(
        dto.CheckinDTO(ServiceType.foursquare,
            "test1a",
            GeodataDTO(40.7505800, -73.9935800),
            DateTime.now,
            "ben1234",
            Some(ServiceVenueDTO(ServiceType.foursquare, "chi", "Chipotle", location = LocationDTO(GeodataDTO(40.7505800, -73.9935800), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test1",
            GeodataDTO(40.7505800, -73.9935800),
            DateTime.now,
            "ben123",
            Some(ServiceVenueDTO(ServiceType.foursquare, "chi", "Chipotle", location = LocationDTO(GeodataDTO(40.7505800, -73.9935800), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test2",
            GeodataDTO(40.749712, -73.993092),
            DateTime.now,
            "ben123",
            Some(ServiceVenueDTO(ServiceType.foursquare, "gg", "G&G", location = LocationDTO(GeodataDTO(40.0, -74.0000012), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test2b",
            GeodataDTO(40.749712, -73.993092),
            DateTime.now - 1.minute,
            "ben123",
            Some(ServiceVenueDTO(ServiceType.foursquare, "gg", "G&G", location = LocationDTO(GeodataDTO(40.0, -74.0000012), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test2c",
            GeodataDTO(40.750817, -73.992405),
            DateTime.now - 1.minute,
            "ben12345",
            Some(ServiceVenueDTO(ServiceType.foursquare, "cosi", "Cosi", location = LocationDTO(GeodataDTO(40.0, -74.0000013), "x")))
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test3",
            GeodataDTO(40.750183, -73.992512),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test4",
            GeodataDTO(40.750183, -73.992513),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test5",
            GeodataDTO(40.750183, -73.992514),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test5b",
            GeodataDTO(40.791979, -73.957215),
            DateTime.now - 1.minute,
            "ben12345",
            Some(ServiceVenueDTO(ServiceType.foursquare, "xy", "XY", location = LocationDTO(GeodataDTO(40.791979, -73.957214), "x")))
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test5a",
            GeodataDTO(40.791979, -73.957214),
            DateTime.now,
            "ben123",
            None
        )

    ).map(c => c.id -> c), Tuples.CheckinIdDTO)*/

    val placeInferenceOut = Tsv(args("placeInferenceOut"), Tuples.PlaceInference)

    val correlation = SequenceFile("correlationIn", Tuples.Correlation).read
    /*val correlation = IterableSource(Seq(
        ("c1", ServiceType.foursquare, "ben123"),
        ("c1", ServiceType.sonar, "ben123")
    ), Tuples.Correlation)
*/
    val segmentedCheckins = checkinSource.read.flatMapTo(('checkinDto) ->('checkinId, 'serviceProfileId, 'serviceType, 'canonicalVenueId, 'location, 'timeSegment)) {
        dto: CheckinDTO =>
            val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
            val weekDay = isWeekDay(ldt)
            // only treat foursquare venues as venues
            val canonicalVenueId = if (dto.venueId == null || dto.venueId.isEmpty || dto.serviceType != ServiceType.foursquare) null else dto.serviceVenue.canonicalId
            // create tuples for each time segment
            createSegments(ldt.toLocalTime, segments) map {
                segment => (dto.canonicalId, dto.serviceProfileId, dto.serviceType, canonicalVenueId, dto.serviceVenue.location.geodata, TimeSegment(weekDay, segment.name))
            }
    }.leftJoinWithSmaller(('serviceType, 'serviceProfileId) ->('_serviceType, '_serviceProfileId), correlation.read.rename(('serviceType, 'serviceProfileId) ->('_serviceType, '_serviceProfileId))).discard(('_serviceType, '_serviceProfileId)).map(('correlationId, 'serviceType, 'serviceProfileId) -> 'userGoldenId) {
        in: (String, ServiceType, String) =>
        // add some fake correlation id if there is no correlation in cassandra
            val (correlationId, serviceType, serviceProfileId) = in
            if (correlationId == null) serviceType.name() + ":" + serviceProfileId else correlationId
    }

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
    val ClusterSizeInMeters = 200
    // find centroids of clusters within time segments per user
    val clusterAveragesPipe = segmentedCheckins.groupBy('userGoldenId, 'timeSegment) {
        _.mapList(('checkinId, 'location) -> ('adjustedCheckinLocations)) {
            checkins: List[(String, GeodataDTO)] =>
                val geoToCheckinIdMap = checkins.map {
                    case (checkinId, geodata) =>
                        val GeodataDTO(lat, lng) = geodata
                        (lat, lng) -> checkinId
                }.toMap
                val checkinLocations = geoToCheckinIdMap.keySet
                val clusters = LocationClusterer.cluster(checkinLocations, ClusterSizeInMeters, 1)
                val adjustedCheckinLocations = clusters.flatMap {
                    cluster =>
                        val dsValues = LocationClusterer.datasetValues(cluster)
                        val (avgLat, avgLng) = LocationClusterer.average(dsValues)
                        val geodata = GeodataDTO(avgLat, avgLng)
                        // TODO: if we need to map back to checkins: dsValues.map(geoToCheckinIdMap).map(_ -> geodata)
                        Some(geodata -> cluster.size())
                }.toSeq
                //println("Cluster contents:" + adjustedCheckinLocations)
                adjustedCheckinLocations
        }
    }.flatten[(String, GeodataDTO)]('adjustedCheckinLocations ->( /*'checkinId,*/ 'location, 'numLocationVisits)).discard('adjustedCheckinLocations)

    // ensure unique tuples for user-timesegment-location and sum up visits if there are duplicates
    val locations = clusterAveragesPipe.groupBy('userGoldenId, 'timeSegment, 'location) {
        _.sum('numLocationVisits -> 'numVisits)
    }
    val metaFields: Fields = ('canonicalVenueId)
    val checkinIdFields: Fields = ('userGoldenId, 'location, 'timeSegment)
    // match checkins and pongs with places visited by the user
    val withUserPopularity = matchGeo((venueUserPopularity, ('userGoldenId, 'timeSegment), 'venueLocation, 'venueUserPopularity), (locations, ('userGoldenId, 'timeSegment), 'location, checkinIdFields), 'userDistance, metaFields, threshold = ClusterSizeInMeters).rename(('userGoldenId, 'canonicalVenueId, 'location, 'timeSegment) -> (('user_userGoldenId, 'user_canonicalVenueId, 'user_location, 'user_timeSegment)))
    // match checkins and pongs with places visited by all users
    val withVenuePopularity = matchGeo((venuePopularity, 'timeSegment, 'venueLocation, 'venuePopularity), (locations, 'timeSegment, 'location, checkinIdFields), 'allDistance, metaFields append 'numVisits, threshold = ClusterSizeInMeters)
    val topScores = withVenuePopularity.leftJoinWithSmaller(('userGoldenId, 'canonicalVenueId, 'location, 'timeSegment) ->('user_userGoldenId, 'user_canonicalVenueId, 'user_location, 'user_timeSegment), withUserPopularity)
            // create composite score from user and allUser score
            .map(('venueUserPopularity, 'venuePopularity) -> 'score) {
        in: (java.lang.Double, java.lang.Double) =>
        //debug("Scores: " + in)
            if (in._1 == null) in._2 else (in._1 + in._2) / 2.0
    }
            .groupBy('userGoldenId, 'timeSegment, 'location, 'numVisits) {
        // pick the top 3 scores for each checkin
        _.sortWithTake[cascading.tuple.Tuple](('score, 'canonicalVenueId) -> 'topScores, 3) {
            (left: cascading.tuple.Tuple, right: cascading.tuple.Tuple) =>
            //debug("Top Scores: " + left + " " + right)
                left.getDouble(0) > right.getDouble(0)
        }
    }.flatten[cascading.tuple.Tuple]('topScores ->('score, 'canonicalVenueId))

    topScores.write(placeInferenceOut)


}

case class TimeSegment(weekday: Boolean, segment: Int) extends Comparable[TimeSegment] {
    def compareTo(o: TimeSegment) = Ordering[(Boolean, Int)].compare((weekday, segment), (o.weekday, o.segment))
}
