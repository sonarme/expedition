package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding._
import collection.JavaConversions._
import filter.ParseFilterFactory
import org.joda.time.DateTime
import cascading.pipe.joiner.{RightJoin, LeftJoin, OuterJoin}
import org.jsoup.Jsoup
import java.net.URL
import org.apache.commons.validator.routines.UrlValidator
import com.twitter.scalding.Tsv
import cascading.tuple.Fields
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import com.sonar.expedition.scrawler.publicprofile.PublicProfileCrawlerUtils
import util.Random

class Crawler(args: Args) extends Job(args) {

    val level: Int = args("level").toInt
    val levelUp: Int = level + 1
    val outputDir = args("output")
    val domains = args("domains")

    val links = Tsv(outputDir+"/crawl_"+level+"/links.tsv", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val linksOutput = Tsv(outputDir+"/crawl_"+levelUp+"/links.tsv", ('url, 'timestamp, 'referer))
    val status = Tsv(outputDir+"/crawl_"+level+"/status.tsv", ('url, 'status, 'attempts, 'crawlDepth)) //('url, 'status, 'timestamp, 'attempts, 'crawlDepth)
    val statusOutput = Tsv(outputDir+"/crawl_"+levelUp+"/status.tsv", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth))
    val parsed = Tsv(outputDir+"/crawl_"+level+"/parsed.tsv") //('url, 'timestamp, 'businessName, 'category, 'subcategory, 'rating)
    val raw = Tsv(outputDir+"/crawl_"+level+"/raw.tsv") //('url, 'timestamp, 'status, 'content, 'links)
    val dummy = Tsv(outputDir+"/crawl_"+level+"/dummy.tsv")
    val dummy2 = Tsv(outputDir+"/crawl_"+level+"/dummy2.tsv")
    val dummy3 = Tsv(outputDir+"/crawl_"+level+"/dummy3.tsv")

    //Sequence files
    val linksSequence = SequenceFile(outputDir+"/crawl_"+level+"/links", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val linksOutputSequence = SequenceFile(outputDir+"/crawl_"+levelUp+"/links", ('url, 'timestamp, 'referer))
    val statusSequence = SequenceFile(outputDir+"/crawl_"+level+"/status", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth)) //('url, 'status, 'timestamp, 'attempts, 'crawlDepth)
    val statusOutputSequence = SequenceFile(outputDir+"/crawl_"+levelUp+"/status", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth))
    val parsedSequence = SequenceFile(outputDir+"/crawl_"+level+"/parsed", Fields.ALL) //('url, 'timestamp, 'businessName, 'category, 'subcategory, 'rating)
    val rawSequence = SequenceFile(outputDir+"/crawl_"+level+"/raw", ('url, 'timestamp, 'status, 'content, 'links)) //('url, 'timestamp, 'status, 'content, 'links)

    val venues = TextLine("/Users/rogchang/Desktop/venuessorted.txt")

    /*
    val foursquareVenues = venues
        .read
        .filter('line) { goldenId: String => goldenId.startsWith("foursquare")}
        .mapTo('line -> ('url, 'timestamp, 'referer)) { goldenId: String =>  ("https://foursquare.com/v/" + goldenId.split(":").tail.head, new DateTime().getMillis, goldenId)}

    foursquareVenues
        .write(Tsv("/Users/rogchang/Desktop/foursquareLinks.tsv"))

    val facebookVenues = venues
        .read
        .filter('line) { goldenId: String => goldenId.startsWith("facebook")}
        .mapTo('line -> ('url, 'timestamp, 'referer)) { goldenId: String =>  ("http://graph.facebook.com/" + goldenId.split(":").tail.head, new DateTime().getMillis, goldenId)}

    facebookVenues
        .write(Tsv("/Users/rogchang/Desktop/facebookLinks.tsv"))
    */
    /*
    val twitterVenues = venues
            .read
            .filter('line) { goldenId: String => goldenId.startsWith("twitter")}
            .mapTo('line -> ('url, 'timestamp, 'referer)) { goldenId: String =>  ("https://api.twitter.com/1/geo/id/" + goldenId.split(":").tail.head + ".json", new DateTime().getMillis, goldenId)}

        twitterVenues
            .write(Tsv("/Users/rogchang/Desktop/twitterLinks.tsv"))
    */


    //Read from status.tsv and output the fetched urls
    //TODO: find a way to split in one step
    val fetched = status
            .read
            .filter('status) { status: String => status == "fetched" }


    //Read from status.tsv and output the unfetched urls
    val unfetched = status
            .read
            .filter('status) { status: String => status == "unfetched" }


    //get unique unfetched links by joining links and status
    val unfetchedLinks = links
                .read
//                .discard('timestamp)
                .rename('url -> 'unfetchedUrl)
                .joinWithSmaller('unfetchedUrl -> 'url, fetched, joiner = new LeftJoin)
                .filter('status) {status: String => status != "fetched"}
                .project('unfetchedUrl, 'timestamp)

//    unfetchedLinks
//        .write(dummy)


    //get all unfetched links
    val allUnfetched = unfetchedLinks
        .joinWithSmaller('unfetchedUrl -> 'url, unfetched, joiner = new OuterJoin)
        .mapTo(('unfetchedUrl, 'url, 'timestamp) -> ('url, 'timestamp)) { x: (String, String, Long) =>
            val (url, unfetchedUrl, timestamp) = x
            if (url != null)
                (url, timestamp)
            else
                (unfetchedUrl, timestamp)
        }

//    allUnfetched
//        .write(dummy2)


    //foreach allUnfetched -> fetch content and write to raw and parsed
    val rawTuples = allUnfetched
            .map('url -> ('status, 'content, 'links)) { url: String => {
                    val httpclient = new DefaultHttpClient
                    val method = new HttpGet(url)
                    method.setHeader("User-Agent", PublicProfileCrawlerUtils.USER_AGENT_LIST(Random.nextInt((PublicProfileCrawlerUtils.USER_AGENT_LIST.size) - 1)))
                    val (status, content) = try {
                        println("fetching: " + url)
                        val rnd = new Random()
                        val range = 1000 to 3000 // randomly sleep btw 1s to 3s
                        Thread.sleep(range(rnd.nextInt(range length)))
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
                                }).filter(d => {
                                    domains.split(",").foldLeft(false)(_ || d.indexOf(_) > -1)
                                })
                            } else List.empty[String]
                        }
                        case _ => List.empty[String]
                    }
                    (status, content, links)
                }
            }

//    rawTuples
//        .write(raw)

    rawTuples
        .write(rawSequence)

    /*
    //Write outgoing links from rawTuples to next links level
    val outgoingLinks = rawTuples
            .flatMapTo('links -> 'link) { links : Iterable[String] => links.filter(link => link != null && link != "") }
            .unique('link)
            .mapTo('link -> ('url, 'timestamp, 'referer)) { link: String => (link, new DateTime().getMillis, "referer")}

    outgoingLinks
        .write(linksOutput)
    */

    //Parse out the content and write to parsed.tsv
    val parsedTuples = rawTuples
            .filter('url) { url: String => url != null && ParseFilterFactory.getParseFilter(url).isIncluded(url)}
            .map(('url, 'content) -> ('businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'likes, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage)) { in: (String, String) => {
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

                    (business, category, rating, latitude, longitude, address, city, state, zip, phone, priceRange, reviewCount, likes, dealPrice, purchased, savingsPercent, dealDescription, dealImage)
                }
            }
            .discard('content, 'links, 'status)

    parsedTuples
        .write(parsed)

    parsedTuples
        .write(parsedSequence)

    //Write status of each fetch
    /*
    val statusOut = rawTuples
            .mapTo(('url, 'status, 'timestamp) -> ('url, 'status, 'timestamp, 'attempts, 'crawlDepth)){ x: (String, String, Long) => {
                val (url, status, timestamp) = x
                (url, status, timestamp, 1, level)
            }
    }

    statusOut
        .write(statusOutput)
    */
}