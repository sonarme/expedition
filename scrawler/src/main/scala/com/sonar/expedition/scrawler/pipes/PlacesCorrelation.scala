package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Tsv, TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.dto.{ServiceType, Priorities}

import cascading.tuple.Fields

trait PlacesCorrelation extends CheckinGrouperFunction with LocationBehaviourAnalysePipe {
    val PlaceCorrelationSectorSize = 30

    def getVenueType(venue1: String, venue2: String): String = if (venue2 != null || venue2 != "") venue2 else venue1

    def placeClassification(checkins: RichPipe, bayestrainingmodel: String, placesData: String) = {
        val newCheckins = correlationCheckinsFromCassandra(checkins)
        val placesVenueGoldenIdValues = withGoldenId(newCheckins)
        //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)

        // module : start of determining places type from place name

        val placesClassified = classifyPlaceType(bayestrainingmodel, placesVenueGoldenIdValues)
                //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName,'venTypeFromModel, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                .map('venTypeFromModel -> 'venTypeFromPlacesData) {
            venTypeFromModel: String => ""
        }


        val placesPipe = getLocationInfo(TextLine(placesData).read)
                .project(('geometryLatitude, 'geometryLongitude, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory))


        placesVenueGoldenIdValues
                .leftJoinWithSmaller('venName -> 'propertiesName, placesPipe)
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'classifiersCategory, 'geometryLatitude, 'geometryLongitude, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                .map('classifiersCategory ->('venTypeFromModel, 'venTypeFromPlacesData)) {

            classifiersCategory: String => ("", classifiersCategory)
        }
                .map(('geometryLatitude, 'geometryLongitude, 'lat, 'lng) -> 'distance) {
            fields: (java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double) =>
                if (fields._1 != null && fields._2 != null && fields._3 != null && fields._4 != null)
                    Haversine.haversine(fields._1, fields._2, fields._3, fields._4)
                else -1
        }
                .groupBy('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId, 'venTypeFromModel, 'venTypeFromPlacesData) {
            _.min('distance)
        }.filter('distance) {
            distance: Double => distance != -1
        }.discard('distance)
                .++(placesClassified)
                .map(('venTypeFromModel, 'venTypeFromPlacesData) -> 'venueType) {

            in: (String, String) =>
                val (venTypeFromModel, venTypeFromPlacesData) = in
                getVenueType(venTypeFromModel, venTypeFromPlacesData)

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

    def correlatedPlaces(checkins: RichPipe): RichPipe =
        checkins.flatMap(('lat, 'lng, 'venName) ->('geosector, 'stemmedVenName)) {
            // add geosector and stemmed venue name
            fields: (Double, Double, String) =>
                val (lat, lng, venName) = fields
                val stemmedVenName = StemAndMetaphoneEmployer.getStemmed(venName)
                if (CommonFunctions.isNullOrEmpty(venName) && CommonFunctions.isNullOrEmpty(stemmedVenName)) None
                else {
                    val geosector = GeoHash.withBitPrecision(lat, lng, PlaceCorrelationSectorSize).longValue()
                    Some((geosector, stemmedVenName))
                }

        }.groupBy('serType, 'venId) {
            // dedupe
            _.head('venName, 'stemmedVenName, 'geosector)

        }.groupBy('stemmedVenName, 'geosector) {
            // correlate
            _.sortWithTake(('venId, 'serType, 'venName) -> 'groupData, 4) {
                (venueId1: (String, String, String), venueId2: (String, String, String)) =>
                    CommonFunctions.venueGoldenIdPriorities(ServiceType.valueOf(venueId1._2)) > CommonFunctions.venueGoldenIdPriorities(ServiceType.valueOf(venueId2._2))
            }

        }.flatMap('groupData ->('goldenId, 'correlatedVenueIds, 'venueId, 'venueIdService, 'venName)) {
            // flatten
            groupData: List[(String, String, String)] =>
            // remove venName from group data
                val correlatedVenueIds = groupData map {
                    case (venueId, venueIdService, venName) => (venueId, venueIdService)
                }
                // create golden id
                val (venueId, venueIdService, _) = groupData.head
                val goldenId = venueIdService + ":" + venueId
                // create data for flattening
                groupData map {
                    case (venueId, venueIdService, venName) => (goldenId, correlatedVenueIds, venueId, venueIdService, venName)
                }

        }.project('correlatedVenueIds, 'venName, 'stemmedVenName, 'geosector, 'goldenId, 'venueId, 'venueIdService)


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
            venId: String => !CommonFunctions.isNullOrEmpty(venId)
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
    }

}

