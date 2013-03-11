package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.dossier.dto
import dto._
import dto.GeodataDTO
import dto.LocationDTO
import dto.ServiceVenueDTO
import org.scala_tools.time.Imports._
import com.sonar.expedition.scrawler.util.{LocationClusterer, Tuples}
import com.twitter.scalding.SequenceFile
import scala.Some
import com.sonar.expedition.scrawler.util.CommonFunctions.Segment
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.checkins.CheckinInference
import com.sonar.expedition.common.segmentation.TimeSegmentation


class ExtractCentroids(args: Args) extends DefaultJob(args) with CheckinInference with TimeSegmentation {

    val segments = Seq((10, 18, true), (20, 7, false)) map {
        case (fromHr, toHr, name) => Segment(from = fromHr, to = toHr, name = name)
    }
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)
    val checkinSource = if (test) IterableSource(Seq(
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

    ).map(c => c.id -> c), Tuples.CheckinIdDTO)
    else SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO)

    val correlation = if (test) IterableSource(Seq(
        (ServiceProfileLink(ServiceType.sonar, "ben123"), ServiceProfileLink(ServiceType.foursquare, "ben123")),
        (ServiceProfileLink(ServiceType.sonar, "ben123"), ServiceProfileLink(ServiceType.sonar, "ben123"))
    ), Tuples.CorrelationGolden)
    else SequenceFile(args("correlationIn") + "_golden", Tuples.CorrelationGolden)

    val segmentedCheckins = checkinSource.read.flatMapTo(('checkinDto) ->('spl, 'location, 'clusterColumn)) {
        dto: CheckinDTO =>
            if (dto.serviceProfileId == null) Iterable.empty
            else {
                val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
                // create tuples for each time segment
                val timeSegments = createSegments(ldt.toLocalTime.getHourOfDay, segments, Some((24, 0)))
                if (timeSegments.isEmpty) None
                else {
                    val weekDay = isWeekDay(ldt)
                    val isWork = weekDay && timeSegments.map(_.name).forall(identity[Boolean])
                    Some((dto.link, GeodataDTO(dto.latitude, dto.longitude), if (isWork) "work" else "home"))
                }
            }
    }.leftJoinWithSmaller('spl -> 'correlationSPL, correlation.read).discard('correlationSPL).map(('userGoldenSPL, 'spl) -> 'userGoldenId) {
        in: (ServiceProfileLink, ServiceProfileLink) =>
        // add some fake correlation id if there is no correlation in cassandra
            val (correlationId, spl) = in
            if (correlationId == null) spl else correlationId
    }.groupBy('userGoldenId, 'clusterColumn) {
        _.mapList('location -> 'clusterCenter) {
            locations: List[GeodataDTO] =>
                (LocationClusterer.maxClusterCenter(locations.map(location => (location.latitude, location.longitude))) map {
                    case (latitude, longitude) => GeodataDTO(latitude, longitude)
                }).orNull
        }
    }.groupBy('userGoldenId) {
        _.pivot(('clusterColumn, 'clusterCenter) ->('work, 'home))
    } write (SequenceFile(args("centroidOut"), Tuples.Centroid))
}
