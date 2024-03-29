package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.dossier.dto
import com.sonar.dossier.dto._
import org.scala_tools.time.Imports._
import com.sonar.expedition.scrawler.util.{Haversine, Tuples}
import collection.JavaConversions._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sonar.dossier.dto.ServiceVenueDTO
import scala.Some
import com.sonar.dossier.dto.LocationDTO
import com.twitter.scalding.IterableSource
import com.sonar.dossier.dto.GeodataDTO

class FindGeofenceActionJob(args: Args) extends DefaultJob(args) {
    val geofencesIn = args("geofences")
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)
    val checkinsIn = args("checkinsIn")
    val output = args("output")

    val checkinSource = if (test) IterableSource(Seq(
        dto.CheckinDTO(ServiceType.foursquare,
            "walmart0",
            GeodataDTO(42.101077409090805, -71.05758522727374),
            DateTime.now,
            "ben1234",
            Some(ServiceVenueDTO(ServiceType.foursquare, "wm", "Walmart", location = LocationDTO(GeodataDTO(42.101077409090805, -71.05758522727374), "x")))
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
        dto.CheckinDTO(ServiceType.sonar,
            "walmart1",
            GeodataDTO(40.07763212, -74.1174186),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "walmart1exit",
            GeodataDTO(40.750183, -73.992513),
            DateTime.now + 10.minutes,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test5",
            GeodataDTO(40.750183, -73.992514),
            DateTime.now + 30.minutes,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "walmart2",
            GeodataDTO(42.1010, -71.0575),
            DateTime.now + 1.hour,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "walmart2exit",
            GeodataDTO(40.791979, -73.957214),
            DateTime.now + 2.hours,
            "ben123",
            None
        )
    ).map(c => c.id -> c), Tuples.CheckinIdDTO)
    else SequenceFile(checkinsIn, Tuples.CheckinIdDTO)

    //for each sonar checkin, see if it is in the list of walmarts
    val checkinsCondensed = checkinSource
        .filter('checkinDto) {checkinDto: CheckinDTO => checkinDto.serviceType == ServiceType.sonar}
        .mapTo('checkinDto -> ('sonarId, 'checkinTime, 'condensedCheckin)) { checkinDto: CheckinDTO => {
            (checkinDto.serviceProfileId, checkinDto.checkinTime, CondensedCheckin(checkinDto.serviceProfileId, checkinDto.checkinTime, checkinDto.serviceCheckinId, checkinDto.latitude, checkinDto.longitude))
            }
        }

    val checkinsGrouped = checkinsCondensed.groupBy('sonarId) { _.toList[CondensedCheckin](('condensedCheckin) -> 'condensedCheckinList).sortBy('checkinTime).reverse}

        //split list into two...one for walmart checkins and another for anything else
    val checkinsPartitioned = checkinsGrouped.mapTo('condensedCheckinList -> ('targetedPongs, 'otherPongs)) {
            condensedCheckinList: List[CondensedCheckin] => {
                //find each walmart checkin and the checkin after that to use as the exit
                condensedCheckinList.partition { condensedCheckin => {
                        getGeofences().find(wm =>Haversine.haversineInMeters(condensedCheckin.lat, condensedCheckin.lng, wm.get("lat").asDouble(), wm.get("lng").asDouble) <= 150) match {
                            case Some(location) => condensedCheckin.venueId = "factual-" + location.get("id").textValue(); true
                            case None => false
                        }

                    }
                }
            }
        }

    val checkins = checkinsPartitioned.flatMapTo(('targetedPongs, 'otherPongs) -> ('appId, 'platform, 'deviceId, 'geofenceId, 'lat, 'lng, 'entering, 'exiting, 'zone)) {
            in: (List[CondensedCheckin], List[CondensedCheckin]) => {
                val (targetedPongs, otherPongs) = in
                //for each targetedPong...look for a corresponding otherpong which is the first pong after the targeted pong...this is a geofence exit action
                val geofenceActions = targetedPongs.map(tp => (tp, otherPongs.find(tp.checkinTime < _.checkinTime).getOrElse(tp)))
                val dtf = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
                geofenceActions.map{ case(enter, exit) => ("sampler", "ios", enter.sonarId, enter.venueId, enter.lat, enter.lng, enter.checkinTime.withZone(DateTimeZone.UTC).toString(dtf), exit.checkinTime.withZone(DateTimeZone.UTC).toString(dtf), (enter.lng * 24 / 360).toInt)}
            }
        }

    //group the checkins by user
    checkins.write(Csv(output))


    private def getGeofences() = {
        val source = io.Source.fromURL(classOf[FindGeofenceActionJob].getResource("/datafiles/factual/" + geofencesIn + ".json"))
        val locations = source.mkString
        source.close()
        val om = new ObjectMapper
        om.readValue(locations, classOf[JsonNode]).fields().flatMap(_.getValue).toList
    }
}

case class CondensedCheckin(var sonarId: String, var checkinTime: DateTime, var venueId: String, var lat: Double, var lng: Double)