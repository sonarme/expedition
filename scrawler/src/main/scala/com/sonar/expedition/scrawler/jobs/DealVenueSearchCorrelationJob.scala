package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import org.jsoup.Jsoup
import org.apache.commons.validator.routines.UrlValidator
import java.net.URL
import org.joda.time.DateTime
import com.sonar.expedition.scrawler.crawler.{ParseFilter, ExtractorFactory}
import cascading.tuple.Fields
import ch.hsr.geohash.util.VincentyGeodesy
import ch.hsr.geohash.WGS84Point
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import com.sonar.expedition.scrawler.publicprofile.PublicProfileCrawlerUtils
import util.Random
import org.apache.http.util.EntityUtils

class DealVenueSearchCorrelationJob(args: Args) extends Job(args) with ParseFilter {

    val domain = args("domain")
    val outputDir = args("output")

    val deals = Tsv(outputDir + "/dealsWithFirstLinkFromJson.tsv", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'address, 'city, 'state, 'zip, 'lat, 'lng, 'siteLink))

    val raw = Tsv(outputDir + "/raw.tsv") //('url, 'timestamp, 'status, 'content, 'links)
    val parsed = Tsv(outputDir + "/parsed.tsv") //('url, 'timestamp, 'businessName, 'category, 'subcategory, 'rating)

    val parsedSequence = SequenceFile(outputDir + "/parsedSeq", Fields.ALL) //('url, 'timestamp, 'businessName, 'category, 'subcategory, 'rating)
    val rawSequence = SequenceFile(outputDir + "/rawSeq", ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'address, 'city, 'state, 'zip, 'lat, 'lng, 'siteLink, 'content)) //('url, 'timestamp, 'status, 'content, 'links)

    val matchedSequence = SequenceFile(outputDir + "/matchedSeq", Fields.ALL)
    val maybeMatchedSequence = SequenceFile(outputDir + "/maybeMatchedSeq", Fields.ALL)

    val matchedTsv = Tsv(outputDir + "/matched.tsv")
    val maybeMatchedTsv = Tsv(outputDir + "/maybeMatched.tsv")

    val rawTuples = deals
                .map('siteLink -> 'content) { url: String => {
                        val httpclient = new DefaultHttpClient
                        val method = new HttpGet(url)
                        method.setHeader("User-Agent", PublicProfileCrawlerUtils.USER_AGENT_LIST(Random.nextInt((PublicProfileCrawlerUtils.USER_AGENT_LIST.size) - 1)))
                        val (status, content) = try {
                            println("fetching: " + url)
                            Thread.sleep(1000)
                            val response = httpclient.execute(method)
                            val fetchedResult = EntityUtils.toString(response.getEntity)
                            ("fetched", fetchedResult)
                        } catch {
                            case e: Exception => ("error", "")
                        }
                        val links = content match {
                            case s: String if s.length > 0 => {
                                val doc = Jsoup.parse(s)
                                val urlValidator = new UrlValidator()
                                if (urlValidator.isValid(url)) {
                                    def getPortStr(port: Int) = port match {
                                        case -1 => ""
                                        case p => ":" + p
                                    }
                                    val (protocol, host, port) = {
                                        val curUrl = new URL(url)
                                        val p = getPortStr(curUrl.getPort)
                                        (curUrl.getProtocol, curUrl.getHost, p)
                                    }
                                    doc.getElementsByTag("a").map(link => {
                                        //normalize the href
                                        val href = link.attr("href") match {
                                            case a if a.startsWith("/") => protocol + "://" + host + port + a
                                            case c => c
                                        }
                                        //strip query parameters and hashtag
                                        if (urlValidator.isValid(href)) {
                                            val url = new URL(href)
                                            val port = getPortStr(url.getPort)
                                            url.getProtocol + "://" + url.getHost + port + url.getPath
                                        } else {
                                            ""
                                        }
                                    }).filter(_.indexOf(domain) > -1)
                                } else List.empty[String]
                            }
                            case _ => List.empty[String]
                        }
                        content
                    }
                }

        rawTuples
            .write(raw)

//        rawTuples
//            .write(rawSequence)


        //Parse out the content and write to parsed.tsv
        val parsedTuples = rawTuples
                .filter('siteLink) { url: String => url != null && isUrlIncluded(url)}
                .map('content -> ('ybusinessName, 'ycategory, 'yrating, 'ylatitude, 'ylongitude, 'yaddress, 'ycity, 'ystate, 'yzip, 'yphone, 'ypriceRange, 'yreviewCount, 'yreviews)) { content: String => {
                        val extractor = ExtractorFactory.getExtractor(domain, content)
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

                        (business, category, rating, latitude, longitude, address, city, state, zip, phone, priceRange, reviewCount, reviews)
                    }
                }
                .discard('content)
                .map(('merchantName, 'address, 'city, 'state, 'zip, 'lat, 'lng, 'ybusinessName, 'yaddress, 'ycity, 'ystate, 'yzip, 'ylatitude, 'ylongitude) -> 'match) {
                    in: (String, String, String, String, String, Double, Double, String, String, String, String, String, Double, Double) =>
                        val (merchantName, address, city, state, zip, lat, lng, ybusinessName, yaddress, ycity, ystate, yzip, ylatitude, ylongitude) = in
                        //attempt to see if the yelp venue fetched matches the livingsocial venue
                        val matchesCityStateZip = try {
                            city.toLowerCase.equals(ycity.toLowerCase) && state.toLowerCase.equals(ystate.toLowerCase) && zip.toLowerCase.equals(yzip.toLowerCase)
                        } catch {
                            case e: Exception => println(e); println("a:" + city + " " + state + " " + zip + ", b:" + ycity + " " + ystate + " " + yzip); false
                        }
                        val point1 = new WGS84Point(lat, lng)
                        val point2 = new WGS84Point(ylatitude, ylongitude)
                        val distanceMeters = VincentyGeodesy.distanceInMeters(point1, point2)

                        if (distanceMeters < 300)
                            "true"
                        else if (matchesCityStateZip) {
                            if(address == null || yaddress == null || address == "" || yaddress == "")
                                "maybe"
                            else {
                                try {
                                    //attempt to match the number portion of the address
                                    if (address.substring(0, address.indexOf(" ")).equals(yaddress.substring(0, yaddress.indexOf(" "))))
                                        "true"
                                    else if (merchantName.toLowerCase.equals(ybusinessName.toLowerCase))
                                        "true"
                                    else
                                        "maybe"
                                } catch {
                                    case e:Exception => println(e); println("a:" + address + ", b:" + yaddress); "maybe"
                                }
                            }
                        } else
                            "false"
                }

            parsedTuples
                    .filter('match) { matched: String => matched.equals("true")}
                    .discard('match)
                    .write(matchedTsv)

            parsedTuples
                    .filter('match) { matched: String => matched.equals("maybe")}
                    .discard('match)
                    .write(maybeMatchedTsv)
    /*
        parsedTuples
                .filter('match) { matched: String => matched.equals("true")}
                .discard('match)
                .write(matchedSequence)

        parsedTuples
                .filter('match) { matched: String => matched.equals("maybe")}
                .discard('match)
                .write(maybeMatchedSequence)
    */
}