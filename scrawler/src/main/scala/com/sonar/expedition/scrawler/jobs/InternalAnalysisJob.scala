package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.pipes.{ScaldingImplicits, JobImplicits}


trait InternalAnalysisJob extends ScaldingImplicits {


    def internalAnalysisGroupByServiceType(data: RichPipe): RichPipe = {

        val retdata = data.unique('id, 'serviceType).groupBy('serviceType) {
            _.size
        }
        retdata

    }

    def internalAnalysisUniqueProfiles(data: RichPipe): RichPipe = {
        val retdata = data.unique('id).groupAll {
            _.size
        }
        retdata

    }

    def internalAnalysisGroupByCity(joinedProfiles: RichPipe): RichPipe = {
        val returnpipe = joinedProfiles.map('city -> 'cityCleaned) {
            city: String => {
                StemAndMetaphoneEmployer.removeStopWords(city)
            }
        }
                .project(('key, 'cityCleaned))
                .groupBy('cityCleaned) {
            _.size
        }
                .filter('size) {
            size: Int => {
                size > 0
            }
        }.groupAll {
            _.sortBy('size).reverse
        }

        returnpipe
    }

    def internalAnalysisGroupByCityCountryWorktitle(joinedProfiles: RichPipe, placesPipe: RichPipe, jobRunPipeResults: RichPipe, geoHashSectorSize: Int): (RichPipe, RichPipe, RichPipe) = {
        //'key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends

        val returnpipe = internalAnalyisPlaces(joinedProfiles, placesPipe, geoHashSectorSize)

        val returnpipecity = returnpipe.groupBy('propertiesCity) {
            _.size
        }.groupAll {
            _.sortBy('size)
        }

        val returnpipecountry = returnpipe.groupBy('propertiesCountry) {
            _.size
        }.groupAll {
            _.sortBy('size)
        }

        val returnpipework = jobRunPipeResults.groupBy('stemmedWorked) {
            _.size
        }.groupAll {
            _.sortBy('size)
        }
        (returnpipecity, returnpipecountry, returnpipework)
    }


    /*  def internalAnalysisGroupByWorkTitle(joinedProfiles: RichPipe) : RichPipe = {

        }
    */

    def internalAnalyisPlaces(joinedProfiles: RichPipe, placesPipe: RichPipe, geoHashSectorSize: Int): RichPipe = {

        val placespipegeohash = placesPipe.mapTo(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'propertiesProvince, 'propertiesCity, 'propertiesTags, 'propertiesCountry,
                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesPostcode) ->('geometryType, 'geometrygeohash, 'type, 'propertiesProvince, 'propertiesCity, 'propertiesTags, 'propertiesCountry,
                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesPostcode)) {
            fields: (String, String, String, String, String, String, String, String, String, String, String, String, String) =>
                val (properties) = fields


                (properties._1, GeoHash.withBitPrecision(properties._2.toDouble, properties._3.toDouble, geoHashSectorSize * 100).longValue(), properties._4, properties._5, properties._6, properties._7, properties._8, properties._9, properties._10, properties._11, properties._12, properties._13)

        }

        val returnpipe = joinedProfiles.flatMapTo(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends) ->('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'geohash, 'stemmedWorked, 'certaintyScore, 'numberOfFriends)) {
            fields: (String, String, String, String, String, String, String, String, String, String, String) =>
                val (key, uname, fbid, lnid, city, worktitle, lat, lon, stemmedWorked, certaintyScore, numberOfFriends) = fields

                val sectorGeohash = GeoHash.withBitPrecision(lat.toDouble, lon.toDouble, geoHashSectorSize * 100)
                val neighbouringSectors = sectorGeohash.getAdjacent().map(_.longValue())
                neighbouringSectors map (
                        sector => (key, uname, fbid, lnid, city, worktitle, sector, stemmedWorked, certaintyScore, numberOfFriends)
                        )


        }.joinWithLarger('geohash -> 'geometrygeohash, placespipegeohash)
                .project('key, 'uname, 'city, 'worktitle, 'geohash, 'type, 'propertiesProvince, 'propertiesCity, 'propertiesTags, 'propertiesCountry)

        returnpipe
    }
}
