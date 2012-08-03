package com.sonar.expedition.scrawler.apis

import cascading.tuple.Fields
import com.twitter.scalding._
import util.matching.Regex
import APICalls._


class APICalls(args: Args) extends Job(args) {

    def fourSquareCall(workplace: String, locationCityLat: String, locationCityLong: String): String = {
        val resp = new HttpClientRest()
        val location = resp.getFSQWorkplaceLatLongWithKeys(workplace, locationCityLat, locationCityLong);
        location
    }


    def fsqAPIFindLatLongFromCompAndCity(unq_cmp_city: RichPipe): RichPipe = {
        val locationFromCoandCity = unq_cmp_city.unique('mtphnWorked, 'city).mapTo(Fields.ALL ->('work, 'cname, 'lat, 'long)) {
            fields: (String, String) =>
                val (work, city) = fields
                val location = getLatLongCity(city)
                val locationregex(lat, long) = location
                (work, city, lat, long)

        }.project('work, 'cname, 'lat, 'long).mapTo(Fields.ALL ->('workname, 'placename, 'lati, 'longi, 'street_address)) {
            fields: (String, String, String, String) =>
                val (work, city, lat, long) = fields
                if (city == null) {

                    (work, city, "", "", "")
                } else {
                    val locationworkplace = fourSquareCall(work, lat, long)
                    val locationofficeregex(lati, longi, pincode) = locationworkplace
                    (work, city, lati, longi, pincode)
                }

        }.project('workname, 'placename, 'lati, 'longi, 'street_address)

        locationFromCoandCity
    }

    def getLatLongCity(city: String): String = {
        val resp = new HttpClientRest()
        city match {
            case locationregex(lat, lng) => city
            case _ => resp.getLatLong(city)
        }


    }


}

object APICalls {
    val locationregex: Regex = """(.*):(.*)""".r
    val locationofficeregex: Regex = """(.*):(.*):(.*)""".r
}