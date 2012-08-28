package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine, StemAndMetaphoneEmployer}
import cascading.pipe.joiner.{RightJoin, Joiner, LeftJoin}
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.dto.{ServiceType, Priorities}
import PlacesCorrelation._
import JobImplicits._

trait PlacesCorrelation extends ScaldingImplicits {

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
            listOfVenueIds: List[(String, String, String)] => {
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
