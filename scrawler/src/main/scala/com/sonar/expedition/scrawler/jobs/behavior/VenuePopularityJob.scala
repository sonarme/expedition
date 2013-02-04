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
import com.sonar.expedition.scrawler.jobs.{Csv, DefaultJob}
import ch.hsr.geohash.{WGS84Point, BoundingBox}
import com.sonar.expedition.common.segmentation.{TimeSegmentation}

class VenuePopularityJob(args: Args) extends DefaultJob(args) with Normalizers with CheckinInference with TimeSegmentation {
    val segments = Seq(0 -> 8, 7 -> 12, 11 -> 15, 14 -> 17, 16 -> 21, 20 -> 0) map {
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
    val venuePopularityOut = Tsv(args("venuePopularityOut"), ('canonicalVenueId, 'venueName, 'location, 'timeSegment, 'venuePopularity))
    val bb = new BoundingBox(new WGS84Point(47.622364, -122.209854), new WGS84Point(47.608593, -122.184706))
    val segmentedCheckins = checkinSource.read.flatMapTo(('checkinDto) ->('checkinId, 'spl, 'canonicalVenueId, 'venueName, 'location, 'timeSegment)) {
        dto: CheckinDTO =>
            if (dto.serviceProfileId == null || dto.venueId == null || dto.venueId.isEmpty || !bb.contains(dto.geohash.getBoundingBoxCenterPoint)) Iterable.empty
            else {
                val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
                val weekDay = isWeekDay(ldt)
                // only treat foursquare venues as venues
                val canonicalVenueId = dto.serviceVenue.canonicalId
                // create tuples for each time segment
                createSegments(ldt.toLocalTime.getHourOfDay, segments, Some((24, 0))) map {
                    segment => (dto.canonicalId, dto.link, canonicalVenueId, dto.venueName, dto.serviceVenue.location.geodata.canonicalLatLng, TimeSegment(weekDay, segment.name))
                }
            }
    }.groupBy('canonicalVenueId, 'venueName, 'location, 'timeSegment) {
        _.size('venuePopularity)
    }.write(venuePopularityOut)

}

