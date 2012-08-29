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
        val placesVenueGoldenIdValues = correlatedPlaces(newCheckins)
        //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)

        // module : start of determining places type from place name

        val placesClassified = classifyPlaceType(bayestrainingmodel, placesVenueGoldenIdValues)
                //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName,'venTypeFromModel, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
                .map('venTypeFromModel -> 'venTypeFromPlacesData) {
            venTypeFromModel: String => ""
        }


        val placesPipe = getLocationInfo(TextLine(placesData).read)
                .project(('geometryLatitude, 'geometryLongitude, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory))


        placesClassified
                .leftJoinWithSmaller('venName -> 'propertiesName, placesPipe)
                .map(('venTypeFromModel, 'classifiersCategory) -> 'venueType) {

            in: (String, String) =>
                val (venTypeFromModel, venTypeFromPlacesData) = in
                getVenueType(venTypeFromModel, venTypeFromPlacesData)

        }.project('correlatedVenueIds, 'venName, 'stemmedVenName, 'geosector, 'goldenId, 'venueId, 'venueType, 'venueLat, 'venueLng)
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
                val stemmedVenName = StemAndMetaphoneEmployer.removeStopWords(venName)
                if (CommonFunctions.isNullOrEmpty(venName) && CommonFunctions.isNullOrEmpty(stemmedVenName)) None
                else {
                    val geosector = GeoHash.withBitPrecision(lat, lng, PlaceCorrelationSectorSize).longValue()
                    Some((geosector, stemmedVenName))
                }

        }.groupBy('serType, 'venId) {
            // dedupe
            _.head('venName, 'stemmedVenName, 'geosector, 'lat, 'lng)

        }.groupBy('stemmedVenName, 'geosector) {
            // correlate
            _.sortWithTake(('venId, 'serType, 'venName, 'lat, 'lng) -> 'groupData, 4) {
                (in1: (String, String, String, Double, Double), in2: (String, String, String, Double, Double)) =>
                    val venueId1 = in1._1
                    val venueId2 = in2._1
                    val serviceType1 = ServiceType.valueOf(in1._2)
                    val serviceType2 = ServiceType.valueOf(in2._2)
                    CommonFunctions.venueGoldenIdPriorities(serviceType1) > CommonFunctions.venueGoldenIdPriorities(serviceType2) ||
                            serviceType1 == serviceType2 && venueId1.compareTo(venueId2) > 0
            }

        }.flatMap('groupData ->('goldenId, 'correlatedVenueIds, 'venueId, 'venName, 'venueLat, 'venueLng)) {
            // flatten
            groupData: List[(String, String, String, Double, Double)] =>
            // remove venName from group data
                val correlatedVenueIds = groupData map {
                    case (venueId, _, _, _, _) => venueId
                }
                // create golden id
                val goldenId = correlatedVenueIds.head
                // create data for flattening
                groupData map {
                    case (venueId, _, venName, lat, lng) => (goldenId, correlatedVenueIds, venueId, venName, lat, lng)
                }

        }.project('correlatedVenueIds, 'venName, 'stemmedVenName, 'geosector, 'goldenId, 'venueId, 'venueLat, 'venueLng)


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

