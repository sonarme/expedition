package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding._
import collection.JavaConversions._
import filter.ParseFilterFactory
import org.joda.time.DateTime
import cascading.pipe.joiner.{RightJoin, LeftJoin, OuterJoin}
import crawlercommons.fetcher.http.{SimpleHttpFetcher, UserAgent}
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import Crawler._
import org.jsoup.Jsoup
import java.net.URL
import org.apache.commons.validator.routines.UrlValidator
import com.twitter.scalding.Tsv
import cascading.tuple.Fields

class Crawler(args: Args) extends Job(args) {

    val level: Int = args("level").toInt
    val levelUp: Int = level + 1
    val outputDir = args("output")
    val domain = args("domain")

    val links = Tsv(outputDir+"/crawl_"+level+"/links.tsv", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val linksOutput = Tsv(outputDir+"/crawl_"+levelUp+"/links.tsv", ('url, 'timestamp, 'referer))
    val status = Tsv(outputDir+"/crawl_"+level+"/status.tsv", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth)) //('url, 'status, 'timestamp, 'attempts, 'crawlDepth)
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
    val parsedSequence = SequenceFile(outputDir+"/crawl_"+level+"/parsed3", Fields.ALL) //('url, 'timestamp, 'businessName, 'category, 'subcategory, 'rating)
    val rawSequence = SequenceFile(outputDir+"/crawl_"+level+"/raw3", ('url, 'timestamp, 'status, 'content, 'links)) //('url, 'timestamp, 'status, 'content, 'links)


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
                .discard('timestamp)
                .rename('url -> 'unfetchedUrl)
                .joinWithSmaller('unfetchedUrl -> 'url, fetched, joiner = new LeftJoin)
                .filter('status) {status: String => status != "fetched"}
                .project('unfetchedUrl)

    unfetchedLinks
        .write(dummy)


    //get all unfetched links
    val allUnfetched = unfetchedLinks
        .joinWithSmaller('unfetchedUrl -> 'url, unfetched, joiner = new OuterJoin)
        .mapTo(('unfetchedUrl, 'url) -> ('url)) { x: (String, String) =>
            val (url, unfetchedUrl) = x
            if (url != null)
                url
            else
                unfetchedUrl
        }

    allUnfetched
        .write(dummy2)


    //foreach allUnfetched -> fetch content and write to raw and parsed
    val rawTuples = allUnfetched
            .map('url -> ('timestamp, 'status, 'content, 'links)) { url: String => {
                    val fetcher = new SimpleHttpFetcher(1, userAgent)
                    val (status, content) = try {
                        println("fetching: " + url)
                        Thread.sleep(1000)
                        val fetch = fetcher.fetch(url).getContent
                        ("fetched", new String(fetch))
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
                    (new DateTime().getMillis, status, content, links)
                }
            }

//    rawTuples
//        .write(raw)

//    rawTuples
//        .write(rawSequence)

    //Write outgoing links from rawTuples to next links level
    val outgoingLinks = rawTuples
            .flatMapTo('links -> 'link) { links : Iterable[String] => links.filter(link => link != null && link != "") }
            .unique('link)
            .mapTo('link -> ('url, 'timestamp, 'referer)) { link: String => (link, new DateTime().getMillis, "referer")}

    outgoingLinks
        .write(linksOutput)


    //Parse out the content and write to parsed.tsv
    val parsedTuples = rawTuples
            .filter('url) { url: String => ParseFilterFactory.getParseFilter(domain).isIncluded(url)}
            .map('content -> ('businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'reviews)) { content: String => {
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
            .discard('content, 'links, 'status)

    parsedTuples
        .write(parsed)

//    parsedTuples
//        .write(parsedSequence)

    //Write status of each fetch

    val statusOut = rawTuples
            .mapTo(('url, 'status, 'timestamp) -> ('url, 'status, 'timestamp, 'attempts, 'crawlDepth)){ x: (String, String, Long) => {
                val (url, status, timestamp) = x
                (url, status, timestamp, 1, level)
            }
    }

    statusOut
        .write(statusOutput)

}

object Crawler {
    val userAgent = new UserAgent("test", "test@domain.com", "http://test.domain.com")
}