package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding._
import com.sonar.expedition.scrawler.apis.HttpClientRest
import org.json.JSONObject
import io.Source
import com.twitter.scalding.Tsv
import com.sonar.expedition.scrawler.jobs.{DealLocation, DealAnalysis}
import com.fasterxml.jackson.core.`type`.TypeReference
import java.net.URLEncoder
import org.jsoup.Jsoup
import ch.hsr.geohash.WGS84Point
import ch.hsr.geohash.util.VincentyGeodesy
import com.sonar.expedition.scrawler.util.{VenueMatcher, Venue}
import cascading.tuple.Fields
import util.Random
import com.sonar.expedition.scrawler.crawler.Crawler

class YelpCrawl(args: Args) extends Job(args) {

    val outputDir = args("output")
    val src = args("src")

    val deals = Tsv(src, DealAnalysis.DealsOutputTuple)

//    val dealsSample = Tsv(outputDir + "/deals-sample.tsv", ('dealId, 'merchantName, 'address, 'city, 'state, 'zip, 'lat, 'lng))

//    val dealsWithSearchHtmlSeq = Tsv(outputDir + "/dealsWithSearchHtml", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'address, 'city, 'state, 'zip, 'lat, 'lng, 'html))
    val dealsWithSearchHtmlSeq = SequenceFile(outputDir + "/dealsWithSearchHtml", DealAnalysis.DealsOutputTuple append('html))
    val dealsWithFirstLinkFromHtml = Tsv(outputDir + "/dealsWithFirstLinkFromHtml_tsv")
    val dealsWithFirstLinkFromHtmlSeq = SequenceFile(outputDir + "/dealsWithFirstLinkFromHtml")

    val rawSequence = SequenceFile(outputDir + "/raw2", DealAnalysis.DealsOutputTuple append('url, 'status, 'content, 'links))
    val parsed = Tsv(outputDir + "/parsed3_tsv", DealAnalysis.DealsOutputTuple append('url, 'ybusinessName, 'ycategory, 'yrating, 'ylatitude, 'ylongitude, 'yaddress, 'ycity, 'ystate, 'yzip, 'yphone, 'ypriceRange, 'yreviewCount, 'yreviews))
    val parsedSequence = SequenceFile(outputDir + "/parsed3", DealAnalysis.DealsOutputTuple append('url, 'ybusinessName, 'ycategory, 'yrating, 'ylatitude, 'ylongitude, 'yaddress, 'ycity, 'ystate, 'yzip, 'yphone, 'ypriceRange, 'yreviewCount, 'yreviews))

    /*
    val results = deals
                .map(('merchantName, 'merchantAddress, 'merchantCity, 'merchantState, 'merchantZip) -> 'html) {
            in: (String, String, String, String, String) =>
                val (merchantName, address, city, state, zip) = in
                try {
                    val loc = URLEncoder.encode(city + " " + zip, "UTF-8")
                    val url = "http://www.yelp.com/search?find_desc=" + URLEncoder.encode(merchantName, "UTF-8") + "&find_loc=" + loc
                    val (status, content) = Crawler.fetchContent(url)
                    content
                } catch { case e:Exception => println(e); ""}
        }

        results
           .write(dealsWithSearchHtmlSeq)



    val linkOut = results
        .map('html -> 'url) {
        in: String =>
            try{
                val html = in
                val doc = Jsoup.parse(html)
                Option(doc.getElementById("bizTitleLink0")) match {
                    case Some(e) => e.attr("href") match {
                        case a if a.startsWith("/") => "http://www.yelp.com" + a
                        case c => c
                    }
                    case None => ""
                }
            } catch {
                case e: Exception => ""
            }
        }
        .discard('html)
        .filter('url) {url: String => url.nonEmpty}

//    linkOut
//        .write(dealsWithFirstLinkFromHtml)

    val rawTuples = linkOut
        .map('url -> ('status, 'content, 'links)) { url: String => {
                Crawler.fetchToTuple(url)
            }
        }

    rawTuples
        .write(rawSequence)
    */

    rawSequence
        .filter('content) {content: String => content.contains("Sorry, you're not allowed to access this page")}
        .discard('status, 'content, 'links)
        .write(Tsv(outputDir + "/dealsToCrawl"))

    /*
    val parsedTuples = rawSequence
            .map(('url, 'content) -> ('ybusinessName, 'ycategory, 'yrating, 'ylatitude, 'ylongitude, 'yaddress, 'ycity, 'ystate, 'yzip, 'yphone, 'ypriceRange, 'yreviewCount, 'yreviews)) { in: (String, String) => {
                    val (url, content) = in
                    val extractor = ExtractorFactory.getExtractor(url, content)
                    val business = extractor.businessName()
                    val category = extractor.category()
                    val rating = extractor.rating()
                    val latitude = extractor.latitude()
                    val longitude = extractor.longitude()
                    val address = extractor.address()
                    val city = extractor.city()
                    val state = extractor.state()
                    val zip = extractor.zip()
                    val phone = extractor.phone()
                    val priceRange = extractor.priceRange()
                    val reviewCount = extractor.reviewCount()
                    val reviews = extractor.reviews()
                    val peopleCount = extractor.peopleCount()
                    val checkins = extractor.checkinCount()
                    val wereHereCount = extractor.wereHereCount()
                    val talkingAboutCount = extractor.talkingAboutCount()
                    val likes = extractor.likes()
                    val dealPrice = extractor.price()
                    val purchased = extractor.purchased()
                    val savingsPercent = extractor.savingsPercent()
                    val dealDescription = extractor.dealDescription()
                    val dealImage = extractor.dealImage()
                    val dealRegion = extractor.dealRegion()

                    (business, category, rating, latitude, longitude, address, city, state, zip, phone, priceRange, reviewCount, reviews)
                }
            }
            .discard('content, 'links, 'status)
            .map(('merchantName, 'merchantAddress, 'merchantCity, 'merchantState, 'merchantZip, 'venueLat, 'venueLng, 'ybusinessName, 'yaddress, 'ycity, 'ystate, 'yzip, 'ylatitude, 'ylongitude) -> 'match) {
                    in: (String, String, String, String, String, Double, Double, String, String, String, String, String, Double, Double) =>
                        val (merchantName, address, city, state, zip, lat, lng, ybusinessName, yaddress, ycity, ystate, yzip, ylatitude, ylongitude) = in
                        //attempt to see if the yelp venue fetched matches the livingsocial venue
                        val venue1 = new Venue(merchantName, address, city, state, zip, lat, lng)
                        val venue2 = new Venue(ybusinessName, yaddress, ycity, ystate, yzip, ylatitude, ylongitude)
                        VenueMatcher.matches(venue1, venue2)
            }
            .filter('match) { matched: Boolean => matched}
            .discard('match)

    parsedTuples
        .write(parsed)

    parsedTuples
        .write(parsedSequence)
    */
}