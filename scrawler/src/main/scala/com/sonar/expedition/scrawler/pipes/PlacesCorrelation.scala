package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.dto.{ServiceType, Priorities}
import PlacesCorrelation._
import JobImplicits._

trait PlacesCorrelation extends CheckinGrouperFunction with LocationBehaviourAnalysePipe {

    def getVenueType(venue1: String, venue2: String): String = if (venue2 != null || venue2 != "") venue2 else venue1

    def placeClassification(checkins: RichPipe, bayestrainingmodel: String, placesData: String) = {
        val newCheckins = correlationCheckinsFromCassandra(checkins)
        val placesVenueGoldenIdValues = withGoldenId(newCheckins)
        //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)

        // module : start of determining places type from place name

        val placesClassified = classifyPlaceType(bayestrainingmodel, placesVenueGoldenIdValues)
                //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName,'venTypeFromModel, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                .mapTo(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)) {
            fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>

                (fields._1, fields._2, fields._3, fields._4, fields._5, fields._6, "", fields._7, fields._8, fields._9, fields._10, fields._11, fields._12, fields._13, fields._14, fields._15, fields._16)

        }


        val placesPipe = getLocationInfo(TextLine(placesData).read)
                .project(('geometryLatitude, 'geometryLongitude, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory))


        placesVenueGoldenIdValues.leftJoinWithSmaller('venName -> 'propertiesName, placesPipe)
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'classifiersCategory, 'geometryLatitude, 'geometryLongitude, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                .mapTo(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'classifiersCategory, 'geometryLatitude, 'geometryLongitude, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                ->
                ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'geometryLatitude, 'geometryLongitude, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId, 'distance)) {

            fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>
                val distance = {
                    if (fields._7 != null && fields._8 != null && fields._11 != null && fields._12 != null) {
                        Haversine.haversine(fields._7.toDouble, fields._8.toDouble, fields._11.toDouble, fields._12.toDouble)
                    }
                    else {
                        -1
                    }
                }
                (fields._1, fields._2, fields._3, fields._4, fields._5, "", fields._6, fields._7, fields._8, fields._9, fields._10, fields._11, fields._12, fields._13, fields._14, fields._15, fields._16, fields._17, fields._18, distance)

        }
                .groupBy('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId) {
            _.min('distance)
        }.filter('distance) {
            distance: String => distance != "-1"
        }.discard('distance)
                .++(placesClassified)
                .mapTo(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                ->
                ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venueType, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)) {
            fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>

                (fields._1, fields._2, fields._3, fields._4, fields._5, getVenueType(fields._6, fields._7), fields._8, fields._9, fields._10, fields._11, fields._12, fields._13, fields._14, fields._15, fields._16, fields._17)

        }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venueType, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
    }

    def addVenueIdToCheckins(oldCheckins: RichPipe, newCheckins: RichPipe): RichPipe = {
        val newCheckinGrouperPipe = newCheckins
        val oldCheckinGrouperPipe = oldCheckins

        val newCheckinPipe = newCheckinGrouperPipe
                .rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour) ->
                ('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'venId, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour))
                .project('newkeyid, 'newserType, 'newserProfileID, 'newserCheckinID, 'newvenName, 'newvenAddress, 'venId, 'newchknTime, 'newghash, 'newlat, 'newlng, 'newdayOfYear, 'newdayOfWeek, 'newhour)

        val oldCheckinPipe = oldCheckinGrouperPipe
                .joinWithSmaller(('serCheckinID) -> ('newserCheckinID), newCheckinPipe)
                .unique('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour)
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
                (!CommonFunctions.isNullOrEmpty(venueName) || !CommonFunctions.isNullOrEmpty(stemmedVenueName))
            }
        }
                .map(('lat, 'lng) -> 'geosector) {
            fields: (String, String) =>
                val (lat, lng) = fields
                val geosector = GeoHash.withBitPrecision(lat.toDouble, lng.toDouble, PlaceCorrelationSectorSize)
                geosector.longValue()
        }
                .groupBy('serType, 'venId) {
            _.head('venName, 'stemmedVenName, 'geosector)
        }
                .groupBy('stemmedVenName, 'geosector) {
            _

                    .sortWithTake(('venId, 'serType, 'venName) -> 'correlatedVenueIds, 4) {
                (venueId1: (String, String, String), venueId2: (String, String, String)) => CommonFunctions.venueGoldenIdPriorities(ServiceType.valueOf(venueId1._2)) > CommonFunctions.venueGoldenIdPriorities(ServiceType.valueOf(venueId2._2))
            }

        }.map('correlatedVenueIds -> ('goldenId)) {
            listOfVenueIds: List[(String, String)] => {
                val goldenId = listOfVenueIds.head
                goldenId._2 + ":" + goldenId._1
            }
        }
                .flatMap('correlatedVenueIds ->('venueId, 'venueIdService, 'venName)) {
            listOfVenueIds: List[(String, String, String)] => {
                listOfVenueIds
            }
        }
                .project('correlatedVenueIds, 'venName, 'stemmedVenName, 'geosector, 'goldenId, 'venueId, 'venueIdService)

        withGoldenId
    }

    def withGoldenId(oldCheckins: RichPipe, newCheckins: RichPipe): RichPipe = {
        val checkinsWithVenueId = addVenueIdToCheckins(oldCheckins, newCheckins)
        val venueWithGoldenId = correlatedPlaces(checkinsWithVenueId)
        venueWithGoldenId.project('venueId, 'goldenId).joinWithSmaller('venueId -> 'venId, checkinsWithVenueId)
                .filter('venueId) {
            fields: (String) =>
                val venId = fields
                (!CommonFunctions.isNullOrEmpty(venId))
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
    }

    def withGoldenId(newCheckins: RichPipe): RichPipe = {
        val venueWithGoldenId = correlatedPlaces(newCheckins)
        venueWithGoldenId.project('venueId, 'goldenId).joinWithLarger('venueId -> 'venId, newCheckins)
                .filter('venueId) {
            fields: (String) =>
                val venId = fields
                (!CommonFunctions.isNullOrEmpty(venId))
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
    }

}

object PlacesCorrelation {
    val PlaceCorrelationSectorSize = 35
}
