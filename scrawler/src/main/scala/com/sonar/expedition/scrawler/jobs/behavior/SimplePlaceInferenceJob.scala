package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.dossier.dto._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.dossier.{dto, Normalizers}
import com.sonar.expedition.scrawler.checkins.CheckinInference
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.util.CommonFunctions.Segment
import com.twitter.scalding.IterableSource
import com.sonar.dossier.dto.GeodataDTO
import org.scala_tools.time.Imports._
import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.expedition.common.segmentation.TimeSegmentation
import ch.hsr.geohash.{WGS84Point, BoundingBox, GeoHash}
import collection.JavaConversions._
import scalaz._
import Scalaz._
import javax.crypto.spec.SecretKeySpec
import javax.crypto.Cipher
import com.sonar.expedition.scrawler.Encryption._

class SimplePlaceInferenceJob(args: Args) extends DefaultJob(args) with Normalizers with CheckinInference with TimeSegmentation {
    val segments = Seq(1 -> 7, 7 -> 11, 11 -> 14, 14 -> 16, 16 -> 20, 20 -> 1) map {
        case (fromHr, toHr) => Segment(from = fromHr, to = toHr, name = fromHr + "-" + toHr)
    }
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)
    val checkinSource = if (test) IterableSource(Seq(

        dto.CheckinDTO(ServiceType.sonar,
            "corner1",
            GeodataDTO(40.745241, -73.982942),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "corner2",
            GeodataDTO(40.744575, -73.983028),
            DateTime.now,
            "ben123",
            None
        )

    ).map(c => c.id -> c), Tuples.CheckinIdDTO)
    else SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO)
    val checkins = checkinSource.read
    val bb = new BoundingBox(new WGS84Point(24.5210, -124.7625), new WGS84Point(49.3845, -66.9326))
    val segmentedCheckins = checkins.flatMapTo(('checkinDto) ->('checkinId, 'spl, 'location, 'timeSegment, 'geosector)) {
        dto: CheckinDTO =>
            if (dto.serviceProfileId == null || dto.serviceCheckinId == null || dto.serviceType != ServiceType.sonar || !bb.contains(dto.geohash.getBoundingBoxCenterPoint)) Iterable.empty
            else {
                val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
                val weekDay = isWeekDay(ldt)
                val centerSector = smallSector(dto.latitude, dto.longitude)
                // create tuples for each time segment
                for (segment <- createSegments(ldt.toLocalTime.getHourOfDay, segments, Some((24, 0)));
                     geosector <- centerSector.getAdjacent :+ centerSector) yield
                    (dto.serviceType.name() + "-" + encrypt(dto.serviceCheckinId), dto.serviceType.name() + "-" + encrypt(dto.serviceProfileId), dto.serviceVenue.location.geodata.canonicalLatLng, TimeSegment(weekDay, segment.name).toIndexableString, geosector.longValue())
            }
    }

    val venues = if (test) IterableSource(Seq(
        ("gg", ServiceVenueDTO(ServiceType.foursquare, "gg", "gg", location = LocationDTO(GeodataDTO(40.744916, -73.982599), "x"), category = Seq("coffee"))),
        ("dd", ServiceVenueDTO(ServiceType.foursquare, "dd", "dd", location = LocationDTO(GeodataDTO(40.744835, -73.982706), "x"), category = Seq("coffee", "bagels"))),
        ("hp24", ServiceVenueDTO(ServiceType.foursquare, "hp24", "hp24", location = LocationDTO(GeodataDTO(40.745144, -73.983006), "x"), category = Seq("hair")))
    ), Tuples.VenueIdDTO)
    else SequenceFile(args("venuesIn"), Tuples.VenueIdDTO)

    val sectorizedVenues = venues.read.flatMapTo(('venueDto) ->('geosector, 'venueType)) {
        dto: ServiceVenueDTO =>
            if (dto.serviceType == ServiceType.foursquare && dto.getCategory != null)
                dto.getCategory.map {
                    category: String => (smallSector(dto.location.geodata.latitude, dto.location.geodata.longitude).longValue(), category)
                }
            else Iterable.empty
    }.groupBy('geosector) {
        _.foldLeft('venueType -> 'venueTypes)(Map.empty[String, Int]) {
            (agg: Map[String, Int], venueType: String) => agg |+| Map(venueType -> 1)

        }
    }
    val placeInference = segmentedCheckins.joinWithSmaller(('geosector -> 'geosector), sectorizedVenues)
            .groupBy('checkinId, 'spl, 'location, 'timeSegment) {
        _.foldLeft('venueTypes -> 'placeInference)(Map.empty[String, Int]) {
            (agg: Map[String, Int], venueTypes: Map[String, Int]) => agg |+| venueTypes

        }
    }
    val placeInferenceOut = Tsv(args("placeInferenceOut"))
    placeInference.write(placeInferenceOut)

    def smallSector(lat: Double, lng: Double) = GeoHash.withBitPrecision(lat, lng, 8 * 5 - 2)


}
