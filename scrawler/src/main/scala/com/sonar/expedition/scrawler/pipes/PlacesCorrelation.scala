package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, TextLine, Job, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.dto.{ServiceType, Priorities}

class PlacesCorrelation(args: Args) extends Job(args) {

    def correlatedPlaces(oldCheckins: RichPipe, newCheckins: RichPipe): RichPipe = {

        //        val dtoPlacesInfoPipe = places
        val newCheckinGrouperPipe = newCheckins
        val oldCheckinGrouperPipe = oldCheckins

        val newCheckinPipe = newCheckinGrouperPipe
                .rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour) ->
                ('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'venId, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour))
                .project('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'venId, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour)

        val oldCheckinPipe = oldCheckinGrouperPipe
                .joinWithSmaller('serCheckinID -> 'newserCheckinID, newCheckinPipe)
                .discard(('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour))
                .map('venName -> 'stemmedVenName) {
            fields: (String) =>
                val (venName) = fields
                val stemmedVenName = StemAndMetaphoneEmployer.getStemmed(venName)
                (stemmedVenName)
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

        }.map('correlatedVenueIds ->('goldenId)) {
            listOfVenueIds: List[(String, String)] => {
                val goldenId = listOfVenueIds.head
                goldenId._2 + ":" + goldenId._1
            }
        }
                .flatMap('correlatedVenueIds -> ('venueId, 'venueIdService)) {
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

        oldCheckinPipe
    }

}


//        val placesPipe = dtoPlacesInfoPipe
//                .project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
//                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
//                .map('propertiesName -> ('stemmedName)) {
//            fields: String =>
//                val (placeName) = fields
//                val stemmedName = StemAndMetaphoneEmployer.getStemmed(placeName)
//                (stemmedName)
//        }.project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'stemmedName, 'propertiesTags, 'propertiesCountry,
//                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
//                .joinWithSmaller('stemmedName -> 'stemmedVenName, checkinPipe, joiner = new RightJoin)
//                .filter(('lat, 'lng, 'geometryLatitude, 'geometryLongitude)) {
//            fields: (String, String, String, String) =>
//                val (lat, lng, placeLat, placeLng) = fields
//                var havDistance = 0.0
//                if (placeLat == null) {
//                    havDistance == -5.0
//                }
//                else if (lat == null) {
//                    havDistance = -1.0
//                }
//                else {
//                    havDistance = Haversine.haversine(lat.toDouble, lng.toDouble, placeLat.toDouble, placeLng.toDouble)
//                }
//                (((havDistance >= 0.0) && (havDistance <= .2)) || (havDistance == -5.0))
//        }
//                .filter(('stemmedName, 'stemmedVenName)) {
//            fields: (String, String) =>
//                val (place, checkin) = fields
//                ((place != "") && (checkin != ""))
//        }
//                //            .unique('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'stemmedVenName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'id)
//                .groupBy() {
//            _.sortBy('stemmedVenName)
//        }
//                .map(('serType, 'venId, 'id) -> 'goldenId) {
//            fields: (String, String, String) =>
//                val (service, venId, id) = fields
//                var goldenId = ""
//                if (service == "foursquare") {
//                    goldenId = venId + "_4s"
//                }
//                else if (service == "facebook") {
//                    goldenId = venId + "_fb"
//                }
//                else if (service == "twitter") {
//                    goldenId = venId + "_tw"
//                }
//                else {
//                    goldenId = id + "_og"
//                }
//                goldenId
//        }
//                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'stemmedVenName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId)
//        placesPipe
//    }

//    dr5rus7s
//}

//object PlacesCorrelation {
//
//}
