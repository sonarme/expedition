package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, TextLine, Job, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.dto.{ServiceType, Priorities}

class PlacesCorrelation(args: Args) extends Job(args) {

    def addVenueIdToCheckins(oldCheckins: RichPipe, newCheckins: RichPipe): RichPipe = {
        val newCheckinGrouperPipe = newCheckins
        val oldCheckinGrouperPipe = oldCheckins

        val newCheckinPipe = newCheckinGrouperPipe
                .rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour) ->
                ('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'venId, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour))
                .project('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'venId, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour)

        val oldCheckinPipe = oldCheckinGrouperPipe
                .joinWithSmaller('serCheckinID -> 'newserCheckinID, newCheckinPipe)
                .discard(('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour))
        oldCheckinPipe
    }

    def correlatedPlaces(checkins: RichPipe): RichPipe = {
        val withGoldenId = checkins
                .map('venName -> 'stemmedVenName) {
            fields: (String) =>
                val (venName) = fields
                val stemmedVenName = StemAndMetaphoneEmployer.getStemmed(venName)
                (stemmedVenName)
        }
                .filter('venName, 'stemmedVenName) {
            fields: (String, String) => {
                val (venueName, stemmedVenueName) = fields
                (venueName != "" || stemmedVenueName != "")
            }
        }
                .map(('lat, 'lng) -> 'geosector) {
            fields: (String, String) =>
                val (lat, lng) = fields
                val geosector = GeoHash.withCharacterPrecision(lat.toDouble, lng.toDouble, 7)
                geosector.longValue()
        }
                .groupBy('serType, 'venId) {
            _.head('venName, 'geosector)
        }
                .groupBy('venName, 'geosector) {
            _

                    .sortWithTake(('venId, 'serType) -> 'correlatedVenueIds, 4) {
                (venueId1: (String, String), venueId2: (String, String)) => CommonFunctions.venueGoldenIdPriorities(ServiceType.valueOf(venueId1._2)) > CommonFunctions.venueGoldenIdPriorities(ServiceType.valueOf(venueId2._2))
            }

        }.map('correlatedVenueIds -> ('goldenId)) {
            listOfVenueIds: List[(String, String)] => {
                val goldenId = listOfVenueIds.head
                goldenId._2 + ":" + goldenId._1
            }
        }
                .flatMap('correlatedVenueIds ->('venueId, 'venueIdService)) {
            listOfVenueIds: List[(String, String)] => {
                listOfVenueIds
            }
        }

                /*
                .map('correlatedVenueIds -> 'foursquareVenueId) {
            venueList: List[(String, String)] => {
                venueList.find {
                    venueTuple => venueTuple._2 == "foursquare"
                }.map(_._1).getOrElse("none")
            }
        }
                .map('correlatedVenueIds -> 'twitterVenueId) {
            venueList: List[(String, String)] => {
                venueList.find {
                    venueTuple => venueTuple._2 == "twitter"
                }.map(_._1).getOrElse("none")
            }
        }
                .map('correlatedVenueIds -> 'facebookVenueId) {
            venueList: List[(String, String)] => {
                venueList.find {
                    venueTuple => venueTuple._2 == "facebook"
                }.map(_._1).getOrElse("none")
            }
        }
               */

                /* .toList[String]('venId -> 'venIdList)
              .toList[String]('serType -> 'serTypeList)*/
                /*              .flatMap('correlatedVenueIds ->('foursquareVenueId, 'twitterVenueId, 'facebookVenueId, 'openstreetmapVenueId)) {
                    venueList: List[(String, String)] => {
                        venueList.flatMap {
                            venueListItem: (String, String) => {
                                val foursquareVenueId: String = venueListItem match {
                                    case ("foursquare", venueId: String) => venueId
                                    case _ => ""
                                }
                                val twitterVenueId: String = venueListItem match {
                                    case ("twitter", venueId: String) => venueId
                                    case _ => ""
                                }
                                val facebookVenueId: String = venueListItem match {
                                    case ("facebook", venueId: String) => venueId
                                    case _ => ""
                                }
                                val openstreetmapVenueId: String = venueListItem match {
                                    case ("openstreetmap", venueId: String) => venueId
                                    case _ => ""
                                }

                                Option((foursquareVenueId, twitterVenueId, facebookVenueId, openstreetmapVenueId))
                            }
                        }
                        _.map
                    }
                }*/
                //                .project('correlatedVenueIds, 'venName, 'geosector, 'goldenId, 'foursquareVenueId, 'twitterVenueId, 'facebookVenueId)
                .project('correlatedVenueIds, 'venName, 'geosector, 'goldenId, 'venueId, 'venueIdService)

        withGoldenId
    }

    def withGoldenId(oldCheckins: RichPipe, newCheckins: RichPipe): RichPipe = {
        val checkinsWithVenueId = addVenueIdToCheckins(oldCheckins, newCheckins)
        val venueWithGoldenId = correlatedPlaces(checkinsWithVenueId)
        venueWithGoldenId.project('venueId, 'goldenId).joinWithSmaller('venueId -> 'venId, checkinsWithVenueId)
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
    }

}

