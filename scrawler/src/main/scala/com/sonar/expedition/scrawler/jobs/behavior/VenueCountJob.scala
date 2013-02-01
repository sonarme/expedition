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
import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.expedition.common.segmentation.{TimeSegment, TimeSegmentation}

class VenueCountJob(args: Args) extends DefaultJob(args) with Normalizers with CheckinInference with TimeSegmentation {
    val segments = Seq(1 -> 7, 7 -> 11, 11 -> 14, 14 -> 16, 16 -> 20, 20 -> 1) map {
        case (fromHr, toHr) => Segment(from = fromHr, to = toHr, name = fromHr + "-" + toHr)
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
    val placeInferenceOut = SequenceFile(args("placeInferenceOut"), Tuples.PlaceInference)


    val correlation = if (test) IterableSource(Seq(
        (ServiceProfileLink(ServiceType.sonar, "ben123"), ServiceProfileLink(ServiceType.foursquare, "ben123")),
        (ServiceProfileLink(ServiceType.sonar, "ben123"), ServiceProfileLink(ServiceType.sonar, "ben123"))
    ), Tuples.CorrelationGolden)
    else SequenceFile(args("correlationIn"), Tuples.CorrelationGolden)
    val segmentedCheckins = checkinSource.read.flatMapTo(('checkinDto) ->('checkinId, 'spl, 'canonicalVenueId, 'location, 'timeSegment)) {
        dto: CheckinDTO =>
            if (dto.serviceProfileId == null || dto.venueId == null || dto.venueId.isEmpty || dto.serviceType != ServiceType.foursquare) Iterable.empty
            else {
                val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
                val weekDay = isWeekDay(ldt)
                // only treat foursquare venues as venues
                val canonicalVenueId = dto.serviceVenue.canonicalId
                // create tuples for each time segment
                createSegments(ldt.toLocalTime.getHourOfDay, segments, Some((24, 0))) map {
                    segment => (dto.canonicalId, dto.link, canonicalVenueId, dto.serviceVenue.location.geodata, TimeSegment(weekDay, segment.name))
                }
            }
    }.leftJoinWithSmaller('spl -> 'correlationSPL, correlation.read).discard('correlationSPL).map(('userGoldenSPL, 'spl) -> 'userGoldenId) {
        in: (ServiceProfileLink, ServiceProfileLink) =>
        // add some fake correlation id if there is no correlation in cassandra
            val (correlationId, spl) = in
            if (correlationId == null) spl else correlationId
    }
    segmentedCheckins.groupBy('userGoldenId, 'canonicalVenueId, 'location, 'timeSegment) {
        _.size('numVisits)
    }.map(() -> 'score) {
        _: Unit => 1.0
    }.write(placeInferenceOut)


}
