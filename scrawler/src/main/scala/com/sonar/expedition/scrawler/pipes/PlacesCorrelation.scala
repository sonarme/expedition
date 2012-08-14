package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, TextLine, Job, Args}
import com.sonar.expedition.scrawler.util.{Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}

class PlacesCorrelation(args: Args) extends Job(args) {

    def correlatedPlaces(checkIns: RichPipe, places: RichPipe): RichPipe = {

        val dtoPlacesInfoPipe = places
        val checkinGrouperPipe = checkIns

        val checkinPipe = checkinGrouperPipe
                .map('venName -> 'stemmedVenName) {
            fields: String =>
                val (venName) = fields
                val stemmedVenName = StemAndMetaphoneEmployer.getStemmed(venName)
                (stemmedVenName)
        }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'stemmedVenName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour)

        val placesPipe = dtoPlacesInfoPipe
                .project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
                .map('propertiesName -> ('stemmedName)) {
            fields: String =>
                val (placeName) = fields
                val stemmedName = StemAndMetaphoneEmployer.getStemmed(placeName)
                (stemmedName)
        }.project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'stemmedName, 'propertiesTags, 'propertiesCountry,
                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
                .joinWithSmaller('stemmedName -> 'stemmedVenName, checkinPipe, joiner = new RightJoin)
                .filter(('lat, 'lng, 'geometryLatitude, 'geometryLongitude)) {
            fields: (String, String, String, String) =>
                val (lat, lng, placeLat, placeLng) = fields
                var havDistance = 0.0
                if (placeLat == null) {
                    havDistance == -5.0
                }
                else if (lat == null) {
                    havDistance = -1.0
                }
                else {
                    havDistance = Haversine.haversine(lat.toDouble, lng.toDouble, placeLat.toDouble, placeLng.toDouble)
                }
                (((havDistance >= 0.0) && (havDistance <= .2)) || (havDistance == -5.0))
        }
                .filter(('stemmedName, 'stemmedVenName)) {
            fields: (String, String) =>
                val (place, checkin) = fields
                ((place != "") && (checkin != ""))
        }
                //            .unique('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'stemmedVenName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'id)
                .groupAll {
            _.sortBy('stemmedVenName)
        }
                .map(('serType, 'venId, 'id) -> 'goldenId) {
            fields: (String, String, String) =>
                val (service, venId, id) = fields
                var goldenId = ""
                if (service == "foursquare") {
                    goldenId = venId + "_4s"
                }
                else if (service == "facebook") {
                    goldenId = venId + "_fb"
                }
                else if (service == "twitter") {
                    goldenId = venId + "_tw"
                }
                else {
                    goldenId = id + "_og"
                }
                goldenId
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'stemmedVenName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId)
        placesPipe
    }
}

object PlacesCorrelation {

}
