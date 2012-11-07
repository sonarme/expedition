package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding._
import collection.JavaConversions._
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
import com.sonar.expedition.scrawler.jobs.DealAnalysis
import com.sonar.expedition.scrawler.util.Tuples

class CrawlerJob(args: Args) extends Job(args) with Fetcher with ParseFilter {

    val level: Int = args("level").toInt
    val levelUp: Int = level + 1
    val outputDir = args("output")
    val domains = args("domains")
    val src = args("src")

//    val links = Tsv(outputDir+"/crawl_"+level+"/links.tsv", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val linksOutput = Tsv(outputDir+"/crawl_"+levelUp+"/links.tsv", Tuples.Crawler.Links)
    val status = Tsv(outputDir+"/crawl_"+level+"/status.tsv", ('url, 'status, 'attempts, 'crawlDepth)) //('url, 'status, 'timestamp, 'attempts, 'crawlDepth)
    val statusOutput = Tsv(outputDir+"/crawl_"+levelUp+"/status_tsv", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth))
    val parsed = Tsv(outputDir+"/crawl_"+level+"/parsed_tsv") //('url, 'timestamp, 'businessName, 'category, 'subcategory, 'rating)
    val rawTsv = Tsv(outputDir+"/crawl_"+level+"/rawTsv") //('url, 'timestamp, 'status, 'content, 'links)

    //Sequence files
    val linksSequence = SequenceFile(outputDir+"/crawl_"+level+"/links", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val linksOutputSequence = SequenceFile(outputDir+"/crawl_"+levelUp+"/links", ('url, 'timestamp, 'referer))
    val statusSequence = SequenceFile(outputDir+"/crawl_"+level+"/status", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth)) //('url, 'status, 'timestamp, 'attempts, 'crawlDepth)
    val statusOutputSequence = SequenceFile(outputDir+"/crawl_"+levelUp+"/status", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth))
    val parsedSequence = SequenceFile(outputDir+"/crawl_"+level+"/parsed", Fields.ALL)
    val rawSequence = SequenceFile(outputDir+"/crawl_"+level+"/raw", Tuples.Crawler.Raw)

    val venues = SequenceFile(src, YelpCrawl.DealsOutputTuple)


    val links = venues
        .read
        .filter('goldenId) { goldenId: String => goldenId.startsWith("foursquare")}
        .mapTo('goldenId -> ('url, 'timestamp, 'referer)) { goldenId: String =>  {
                try {
                        ("https://foursquare.com/v/" + goldenId.split(":").tail.head, new DateTime().getMillis, goldenId)
                } catch {
                    case e:Exception => println(goldenId); println(e); ("", "", goldenId)
                }
            }
        }
        .filter('url) { url: String => url.nonEmpty }

    //Read from status.tsv and output the fetched urls
    val fetched = status
            .read
            .filter('status) { status: String => status == "fetched" }


    //Read from status.tsv and output the unfetched urls
    val unfetched = status
            .read
            .filter('status) { status: String => status == "unfetched" }


    //get unique unfetched links by joining links and status
    val unfetchedLinks = links
//                .discard('timestamp)
                .rename('url -> 'unfetchedUrl)
                .joinWithSmaller('unfetchedUrl -> 'url, fetched, joiner = new LeftJoin)
                .filter('status) {status: String => status != "fetched"}
                .project('unfetchedUrl, 'timestamp)


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

    //foreach allUnfetched -> fetch content and write to raw and parsed
    val rawTuples = allUnfetched
            .map('url -> ('status, 'content, 'links)) { url: String => {
                    fetchStatusContentAndLinks(url)
                }
            }

    rawTuples
        .write(rawTsv)

    rawTuples
        .write(rawSequence)


    //Write outgoing links from rawTuples to next links level
    val outgoingLinks = rawTuples
            .flatMapTo('links -> 'link) { links : Iterable[String] => links.filter(link => link != null && link != "") }
            .unique('link)
            .mapTo('link -> ('url, 'timestamp, 'referer)) { link: String => (link, new DateTime().getMillis, "referer")}

    outgoingLinks
        .write(linksOutput)


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