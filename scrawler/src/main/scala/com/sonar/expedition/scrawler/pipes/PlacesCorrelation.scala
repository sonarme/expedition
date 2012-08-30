package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Tsv, TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.dto.{ServiceType, Priorities}

import cascading.tuple.Fields

trait PlacesCorrelation extends CheckinGrouperFunction with LocationBehaviourAnalysePipe {
    val PlaceCorrelationSectorSize = 30

    def placeClassification(newCheckins: RichPipe, bayestrainingmodel: String, placesData: String) = {
        val placesVenueGoldenIdValues = correlatedPlaces(newCheckins)

        val placesClassified = classifyPlaceType(bayestrainingmodel, placesVenueGoldenIdValues)

        val placesPipe = getLocationInfo(TextLine(placesData).read)
                .project('propertiesName, 'classifiersCategory)
                .map('propertiesName -> 'stemmedVenNameFromPlaces) {
            venName: String => StemAndMetaphoneEmployer.removeStopWords(venName)
        }.unique('stemmedVenNameFromPlaces, 'classifiersCategory).groupBy('stemmedVenNameFromPlaces) {
            _.toList[String]('classifiersCategory -> 'classifiersCategories)
        }

        placesClassified
                .leftJoinWithSmaller('stemmedVenName -> 'stemmedVenNameFromPlaces, placesPipe)
                .map(('venTypeFromModel, 'classifiersCategories) -> 'venueType) {

            in: (String, List[String]) =>
                val (venTypeFromModel, venTypeFromPlacesData) = in
                // TODO: allow multiple venue types
                if (venTypeFromPlacesData == null || venTypeFromPlacesData == "") venTypeFromModel else venTypeFromPlacesData.mkString(",")

        }.project('venName, 'stemmedVenName, 'geosector, 'goldenId, 'venueId, 'venueType, 'venueLat, 'venueLng)
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
        checkins.groupBy('venId) {
            // dedupe
            _.head('serType, 'venName, 'lat, 'lng)

        }.flatMap(('lat, 'lng, 'venName) ->('geosector, 'stemmedVenName)) {
            // add geosector and stemmed venue name
            fields: (Double, Double, String) =>
                val (lat, lng, venName) = fields
                val stemmedVenName = StemAndMetaphoneEmployer.removeStopWords(venName)
                if (CommonFunctions.isNullOrEmpty(venName) && CommonFunctions.isNullOrEmpty(stemmedVenName)) None
                else {
                    val geosector = GeoHash.withBitPrecision(lat, lng, PlaceCorrelationSectorSize).longValue()
                    Some((geosector, stemmedVenName))
                }

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

        }.flatMap('groupData ->('goldenId, 'venueId, 'venName, 'venueLat, 'venueLng)) {
            // flatten
            groupData: List[(String, String, String, Double, Double)] =>
            // create golden id
                val goldenId = groupData.head._1
                // create data for flattening
                groupData map {
                    case (venueId, _, venName, lat, lng) => (goldenId, venueId, venName, lat, lng)
                }

        }.project('stemmedVenName, 'geosector, 'goldenId, 'venueId, 'venName, 'venueLat, 'venueLng)


}

