package com.sonar.expedition.scrawler.google

import com.twitter.scalding.{TextLine, Tsv, Job, Args}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.sonar.expedition.scrawler.apis.HttpClientRest
import java.util
import org.json.JSONObject
import org.joda.time.DateTime
import com.sonar.expedition.scrawler.jobs.DealLocation
import com.sonar.expedition.scrawler.google.VenueSearch._

class VenueSearch(args: Args) extends Job(args) {

    val key = args("key")
    val cx = args("cx")
    var outputDir = args("output")

    //get from https://s3.amazonaws.com/scrawler/deals-sample-all.tsv
    val dealsSample = Tsv("src/main/resources/datafiles/deals-sample-all.tsv", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))

    val dealsWithSearchJson = Tsv(outputDir + "/dealsWithSearchJson.tsv", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'address, 'city, 'state, 'zip, 'lat, 'lng, 'json))
    val dealsWithFirstLinkFromJson = Tsv(outputDir + "/dealsWithFirstLinkFromJson.tsv")

    val deals = dealsSample
            .map('locationJSON ->('address, 'city, 'state, 'zip, 'lat, 'lng)) {
        in: String =>
            val locationJSON = in
            val dealLocations = try {
                DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, new TypeReference[util.List[DealLocation]] {})
            } catch {
                case e => throw new RuntimeException("JSON:" + locationJSON, e)
            }
            try {
                val dealLocation = dealLocations.head
                (dealLocation.address, dealLocation.city, dealLocation.state, dealLocation.zip, dealLocation.latitude, dealLocation.longitude)
            } catch {
                case e: Exception => ("", "", "", "", 0.0, 0.0)
            }
    }
            .discard('locationJSON)

    val results = deals
            .map(('merchantName, 'address, 'city, 'state) -> 'json) {
        in: (String, String, String, String) =>
            val (merchantName, address, city, state) = in
            val client = new HttpClientRest
            val query = (merchantName + " " + city + " " + state).toLowerCase.replace(" ", "+")
            println("getting: " + query)
            Thread.sleep(1000)
            val res = client.getresponse("https://www.googleapis.com/customsearch/v1?key=" + key + "&cx=" + cx + "&q=" + query)
            res
    }

    results
            .write(dealsWithSearchJson)


    val linksOut = results
            .map('json -> 'link) {
        in: String =>
            val json = in
            val jsonParsed = new JSONObject(json)
            try {
                val temp = jsonParsed.getJSONArray("items")
                val first = temp.getJSONObject(0)
                first.get("link")
            } catch {
                case e: Exception => ""
            }
    }
            .discard('json)

    linksOut
            .write(dealsWithFirstLinkFromJson)

    /*
    val searchResults = venues
            .read
            .map(('name, 'lat, 'lng) -> 'json) {
        x: (String, Double, Double) => {
            val (name, lat, lng) = x
            val yelp = new Yelp("wfz3OQTn-yo1OxpmN5kMCg", "t_CFiPm2hb6n9V7X2K-A554FDGU", "p9c114Prgf2Cok0CR5tUrPJbyn8pY92Z", "E19PBImqwclXX8AMwfGQIPJwa_M")
            yelp.search(name, lat, lng)
        }
    }

    searchResults
            .write(output)
    */
}

object VenueSearch {
    val DealObjectMapper = new ObjectMapper
    DealObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
}