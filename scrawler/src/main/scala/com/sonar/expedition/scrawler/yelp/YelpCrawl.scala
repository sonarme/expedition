package com.sonar.expedition.scrawler.yelp

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import com.sonar.expedition.scrawler.crawler.Yelp
import reflect.BeanProperty
import java.util
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.yelp.YelpCrawl._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.params.HttpClientParams
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import com.sonar.expedition.scrawler.apis.HttpClientRest
import org.json.JSONObject
import org.joda.time.DateTime

class YelpCrawl(args: Args) extends Job(args) {
    val links = Tsv("/Users/rogchang/Desktop/yelp/crawl_0/links.tsv", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val dummy = TextLine("src/main/resources/datafiles/dummy.txt")
    val venues = Tsv("src/main/resources/datafiles/livingsocial.txt", ('id, 'name, 'lat, 'lng))
    val output = Tsv("src/main/resources/datafiles/livingsocialOutput.tsv", ('id, 'name, 'lat, 'lng, 'json))
    val dealsSample = Tsv("src/main/resources/datafiles/deals-sample-all.tsv", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))
    val venues2 = Tsv("src/main/resources/datafiles/venues.tsv", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON))
    val output2 = Tsv("src/main/resources/datafiles/venuesOutput.tsv")
    val output3 = Tsv("src/main/resources/datafiles/output3.tsv", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'address, 'city, 'state, 'zip, 'lat, 'lng, 'json))
    val output4 = Tsv("src/main/resources/datafiles/output4.tsv")

    val links2 = Tsv("/Users/rogchang/Desktop/yelp/crawl_0/links3.tsv", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val output5 = Tsv("src/main/resources/datafiles/output5.tsv", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'address, 'city, 'state, 'zip, 'lat, 'lng, 'json))
    val output6 = Tsv("src/main/resources/datafiles/output6.tsv")

    //381578	true	Tampa Bay Photo Booths	Services	Rental	295.0	[{"address": "1", "city": "Tampa", "state": "FL", "zip": "11111", "phone": "", "latitude": 27.944107, "longitude": -82.5278359}]

    val deals = dealsSample
            .map('locationJSON ->('address, 'city, 'state, 'zip, 'lat, 'lng)) {
        in: String =>
            val locationJSON = in.replace("}\",\"{", "},{")
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

//    deals.write(output2)

    val results = deals
        .map(('merchantName, 'address, 'city, 'state) -> 'json) {
        in: (String, String, String, String) =>
            val (merchantName, address, city, state) = in
            val client = new HttpClientRest
            val query = (merchantName + " " + city + " " + state).toLowerCase.replace(" ", "+")
            println("getting: " + query)
            Thread.sleep(1000)
            val res = client.getresponse("https://www.googleapis.com/customsearch/v1?key=<key>&cx=<cx>&q=" + query)
            res
        }

    results
        .write(output5)


    val linksOut = output5
        .map('json -> 'link) {
        in: String =>
            val json = in
            val jsonParsed = new JSONObject(json)
            try {
            val temp = jsonParsed.getJSONArray("items")
                val first = temp.getJSONObject(0)
                first.get("link")
            } catch {
                case e:Exception => ""
            }
        }
        .discard('json)

    linksOut
        .write(output6)

    linksOut
        .mapTo('link -> ('url, 'timestamp, 'referer)){ link: String => (link, new DateTime().getMillis, "http://www.yelp.com")}
        .filter('url) {url: String => !url.isEmpty}
        .write(links2)

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

object YelpCrawl {
    val DealObjectMapper = new ObjectMapper
    DealObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
}

case class DealLocation(
                               @BeanProperty address: String,
                               @BeanProperty city: String = null,
                               @BeanProperty state: String = null,
                               @BeanProperty zip: String = null,
                               @BeanProperty phone: String = null,
                               @BeanProperty latitude: Double = 0,
                               @BeanProperty longitude: Double = 0
                               ) {
    def this() = this(null)
}