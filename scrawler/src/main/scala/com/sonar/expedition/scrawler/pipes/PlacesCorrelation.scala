package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Tsv, TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.util.{Levenshtein, CommonFunctions, Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.dto.{ServiceType, Priorities}

import cascading.tuple.Fields
import com.sonar.expedition.scrawler.jobs.CheckinSource

trait PlacesCorrelation extends CheckinGrouperFunction with LocationBehaviourAnalysePipe with CheckinSource {
    val firstNumber = """\s*(\d+)[^\d]*""".r

    def extractFirstNumber(s: String) = s match {
        case firstNumber(numStr) => Some(numStr)
        case _ => None
    }


    val PlaceCorrelationSectorSize = 30

    def placeClassification(newCheckins: RichPipe, bayesmodel: String, placesData: String) = {
        val placesVenueGoldenIdValues = correlatedPlaces(newCheckins)

        val placesClassified = classifyPlaceType(bayesmodel, placesVenueGoldenIdValues)
        placesClassified.rename('venTypeFromModel -> 'venueType).map(() -> 'venuePhone) {
            u: Unit => ""
        }
        /*
        val geoPlaces = placesPipe(TextLine(placesData).read)
                .project('geometryLatitude, 'geometryLongitude, 'propertiesName, 'propertiesAddress, 'propertiesPhone, 'classifiersCategory)
                .rename('propertiesPhone -> 'venuePhone)
                .map('propertiesName -> 'stemmedVenNameFromPlaces) {
            venName: String => StemAndMetaphoneEmployer.removeStopWords(venName)
        }

        placesClassified
                .leftJoinWithSmaller('stemmedVenName -> 'stemmedVenNameFromPlaces, geoPlaces)
                .flatMap(('venueLat, 'venueLng, 'geometryLatitude, 'geometryLongitude) -> 'distance) {
            in: (java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double) =>
                val (lat, lng, geometryLatitude, geometryLongitude) = in
                if (lat == null || lng == null || geometryLatitude == null || geometryLongitude == null) Some(-1)
                else {
                    val distance = Haversine.haversineInMeters(lat, lng, geometryLatitude, geometryLongitude)
                    if (distance > 500) None else Some(distance)
                }
        }.groupBy('venueId) {
            _.sortBy('distance).head('venName, 'stemmedVenName, 'geosector, 'goldenId, 'venAddress, 'venTypeFromModel, 'venueLat, 'venueLng, 'classifiersCategory, 'propertiesAddress, 'venuePhone)
        }.map(('venTypeFromModel, 'classifiersCategory) -> ('venueType)) {

            in: (String, List[String]) =>
                val (venTypeFromModel, venTypeFromPlacesData) = in
                // TODO: allow multiple venue types
                if (venTypeFromPlacesData == null || venTypeFromPlacesData.isEmpty) venTypeFromModel else venTypeFromPlacesData.head

        }*/
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

    def correlatedPlacesSimple(checkins: RichPipe): RichPipe = {
        checkins /*.groupBy('venId) {
              _.head('venAddress, 'serType, 'venName, 'lat, 'lng)
          }*/ .map('venId -> 'goldenId)(identity[String]).rename(('venId, 'lat, 'lng) ->('venueId, 'venueLat, 'venueLng))
                .project('goldenId, 'venueId, 'venAddress, 'venName, 'venueLat, 'venueLng)
    }

    def correlatedPlaces(checkins: RichPipe): RichPipe = {
        val checkins1 = checkins.groupBy('venId) {
            // dedupe
            _.head('serType, 'venName, 'venAddress, 'lat, 'lng)

        }.flatMap(('lat, 'lng, 'venName) ->('geosector, 'stemmedVenName)) {
            // add geosector and stemmed venue name
            fields: (Double, Double, String) =>
                val (lat, lng, venName) = fields
                val stemmedVenName = StemAndMetaphoneEmployer.removeStopWords(venName)
                if (CommonFunctions.isNullOrEmpty(venName) && CommonFunctions.isNullOrEmpty(stemmedVenName)) None
                else {
                    Some((dealMatchGeosector(lat, lng), stemmedVenName))
                }
        }
        val checkins2 = checkins1.filter('serType) {
            serType: String => serType != "foursquare"
        }.discard('venName, 'lat, 'lng).rename(('venId, 'serType, 'venAddress, 'stemmedVenName) ->('venId2, 'serType2, 'venAddress2, 'stemmedVenName2))

        checkins1.filter('serType) {
            serType: String => serType == "foursquare"
        }
                .joinWithLarger('geosector -> 'geosector, checkins2)
                /*.filter('serType, 'serType2) {
            in: (String, String) =>
                in._1 > in._2
        }*/
                .flatMap(('stemmedVenName, 'venAddress, 'stemmedVenName2, 'venAddress2) -> 'score) {
            in: (String, String, String, String) =>
                val (stemmedVenName, venAddress, stemmedVenName2, venAddress2) = in
                val levenshtein = Levenshtein.compareInt(stemmedVenName, stemmedVenName2)
                if (levenshtein > math.min(stemmedVenName.length, stemmedVenName2.length) * .33) None
                else {
                    val houseNumber = extractFirstNumber(venAddress)
                    val score = if (houseNumber.isDefined && houseNumber == extractFirstNumber(venAddress2)) -1
                    else levenshtein
                    Some(score)
                }
        }.groupBy('venId) {
            _.sortBy('score).head('serType, 'venName, 'venAddress, 'lat, 'lng, 'venId2)
        }.rename(('venId, 'venId2, 'lat, 'lng) ->('goldenId, 'venueId, 'venueLat, 'venueLng)).project('goldenId, 'venueId, 'venAddress, 'venName, 'venueLat, 'venueLng)
        /*

         }.groupBy('stemmedVenName, 'venAddress, 'geosector) {
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

                }.project('stemmedVenName, 'geosector, 'goldenId, 'venueId, 'venAddress, 'venName, 'venueLat, 'venueLng)
        */

    }
}

