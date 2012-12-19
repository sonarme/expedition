package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding.{IterableSource, Args, Tsv, Job}
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction
import com.sonar.expedition.scrawler.util.Tuples
import java.util.Date
import collection.JavaConversions._
import org.scala_tools.time.Imports._
import com.sonar.dossier.dto._
import scala.Some
import com.sonar.dossier.dto.LocationDTO
import com.twitter.scalding.Tsv
import com.twitter.scalding.IterableSource
import com.sonar.dossier.dto.GeodataDTO


class PlaceTimeFrequencyJob(args: Args) extends Job(args) with CheckinGrouperFunction {

    val checkinsIn = IterableSource(Seq(
            ("checkin1", CheckinDTO(ServiceType.foursquare,
                "123",
                GeodataDTO(40.0, -74.0),
                DateTime.now,
                "roger",
                Some(ServiceVenueDTO(ServiceType.foursquare, "sonar", "Sonar", location = LocationDTO(GeodataDTO(40.0, -74.0), "x")))
            )),
            ("checkin2", CheckinDTO(ServiceType.foursquare,
                "456",
                GeodataDTO(41.0, -73.0),
                DateTime.now - 8.hours,
                "roger",
                Some(ServiceVenueDTO(ServiceType.foursquare, "nysc", "NYSC", location = LocationDTO(GeodataDTO(41.0, -73.0), "y")))
            ))
        ), Tuples.CheckinIdDTO)

//    val checkinsIn = args("checkins")
//    val venuesIn = args("venues")
    val venuesIn = IterableSource(Seq(
        ("venue1", ServiceVenueDTO(ServiceType.foursquare, "sonar", "Sonar", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = List[String]("office"))),
        ("venue2", ServiceVenueDTO(ServiceType.foursquare, "tracks", "Tracks", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = List[String]("office"))),
        ("venue3", ServiceVenueDTO(ServiceType.foursquare, "nysc", "NYSC", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = List[String]("gym")))
    ), Tuples.VenueIdDTO)
    val checkinProbabilityIn = args("checkinProbability")
    val statsOut = args("statsOut")
    val statsOut2 = args("statsOut2")

    /*
    val checkinsPipe = checkinsIn.read.mapTo(('checkinId, 'checkinDto) -> ('checkinId, 'userGoldenId, 'location)){
        x: (String, CheckinDTO) =>
            val (checkinId, dto) = x
            (checkinId, dto.profileId, dto.serviceVenue.location.geodata)
    }
    */
//    checkinsPipe.write(Tsv(statsOut))



    //join checkinDTO and venueDTO with the input
    val stats = Tsv(checkinProbabilityIn, Tuples.PlaceInference)
        .read
//        .joinWithLarger('checkinId -> 'id, Tsv(checkinsIn, ('id, 'user_id, 'lat, 'lng, 'timestamp)))
//        .discard('id, 'lat, 'lng)
        .rename('venueId -> 'vId)
        .joinWithLarger('vId -> 'venueId, venuesIn)
        .discard('vId)
        .map('venueDto -> ('category)){
            dto: ServiceVenueDTO =>
                dto.category.headOption.orNull
        }
        .discard('venueDto)
        .groupBy('userGoldenId, 'checkinId, 'timeSegment, 'category){_.sum('score)}    //need to do this so we can pivot correctly

    stats.write(Tsv(statsOut))


    val stats2 = stats
         .groupBy('userGoldenId, 'category){ _.pivot(('timeSegment, 'score) -> ('a, 'b), 0.0)}    //todo: replace 'a, 'b with time segments

    stats2.write(Tsv(statsOut2))

}