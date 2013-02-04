package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction
import com.sonar.expedition.scrawler.util.Tuples
import java.util.Date
import collection.JavaConversions._
import org.scala_tools.time.Imports._
import com.sonar.dossier.dto._
import scala.Some
import com.sonar.dossier.dto.LocationDTO
import com.sonar.dossier.dto.GeodataDTO
import com.sonar.dossier.dto.ServiceVenueDTO
import com.sonar.dossier.dto.LocationDTO
import com.twitter.scalding.Tsv
import scala.Some
import com.twitter.scalding.IterableSource
import com.sonar.dossier.dto.GeodataDTO
import cascading.pipe.joiner.LeftJoin
import org.joda.time.DateMidnight
import java.util
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.jobs.DefaultJob
import TimeSegment


class PlaceTimeFrequencyJob(args: Args) extends DefaultJob(args) with CheckinGrouperFunction {
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)

    val venuesIn = if (test) IterableSource(Seq(
        ("sonar", ServiceVenueDTO(ServiceType.foursquare, "sonar", "Sonar", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("office"))),
        ("tracks", ServiceVenueDTO(ServiceType.foursquare, "tracks", "Tracks", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("office"))),
        ("nysc", ServiceVenueDTO(ServiceType.foursquare, "nysc", "NYSC", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("gym"))),
        ("penn", ServiceVenueDTO(ServiceType.foursquare, "penn", "Penn", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("train"))),
        ("esen", ServiceVenueDTO(ServiceType.foursquare, "esen", "Esen", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("deli")))
    ), Tuples.VenueIdDTO)
    else SequenceFile(args("venuesIn"), Tuples.VenueIdDTO)

    val checkinProbabilityIn = if (test) IterableSource(Seq(
        ("roger", "location1", "2", "nysc", "10", new TimeSegment(true, "7")),
        ("roger", "location1", "3", "penn", "4", new TimeSegment(true, "7")),
        ("roger", "location1", "2", "sonar", "1", new TimeSegment(true, "7")),
        ("roger", "location2", "1", "tracks", "8", new TimeSegment(true, "16")),
        ("roger", "location2", "2", "sonar", "2", new TimeSegment(true, "16")),
        ("katie", "location1", "2", "esen", "10", new TimeSegment(true, "16"))
    ), Tuples.PlaceInference)
    else Tsv(args("checkinProbabilityIn"), Tuples.PlaceInference)

    val userPlaceTimeMapOut = args("userPlaceTimeMapOut")

    val stats = checkinProbabilityIn
            .joinWithSmaller('canonicalVenueId -> 'venueId, venuesIn)
            .discard('venueId)
            .flatMap('venueDto -> 'placeType) {
        // TODO: handle multiple categories
        dto: ServiceVenueDTO => if (dto.category == null) None else dto.category.headOption
    }
            .discard('venueDto)
            .groupBy('userGoldenId, 'timeSegment, 'placeType) {
        _.sum('score)
    }

    val stats2 = stats.groupBy('userGoldenId, 'placeType) {
        _.mapList(('timeSegment, 'score) -> ('timeSegments)) {
            timeSegments: List[(TimeSegment, Double)] => timeSegments.toMap
        }
    }

    stats2.write(SequenceFile(userPlaceTimeMapOut, Tuples.Behavior.UserPlaceTimeMap))
}
