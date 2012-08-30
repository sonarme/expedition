package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import collection.JavaConversions._
import org.joda.time.DateTime
import cascading.pipe.joiner.{LeftJoin, OuterJoin}
import crawlercommons.fetcher.http.{SimpleHttpFetcher, UserAgent}
import Crawler._
import org.jsoup.Jsoup

class Crawler(args: Args) extends Job(args) {

    val level: Int = args("level").toInt
    val levelUp: Int = level + 1
    val outputDir = args("output")

    //(url, lastFetched, lastUpdated, lastStatus, crawlDepth)
    val links = Tsv(outputDir+"/crawl_"+level+"/links.tsv", ('url, 'timestamp, 'referer)) //('url, 'timestamp, 'referer)
    val linksOutput = Tsv(outputDir+"/crawl_"+levelUp+"/links.tsv", ('url, 'timestamp, 'referer))
    val status = Tsv(outputDir+"/crawl_"+level+"/status.tsv", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth)) //('url, 'status, 'timestamp, 'attempts, 'crawlDepth)
    val statusOutput = Tsv(outputDir+"/crawl_"+levelUp+"/status.tsv", ('url, 'status, 'timestamp, 'attempts, 'crawlDepth))
    val parsed = Tsv(outputDir+"/crawl_"+level+"/parsed.tsv") //('url, 'timestamp, 'businessName, 'category, 'subcategory, 'rating)
    val raw = Tsv(outputDir+"/crawl_"+level+"/raw.tsv") //('url, 'timestamp, 'content, 'links)
    val dummy = Tsv(outputDir+"/crawl_"+level+"/dummy.tsv")
    val dummy2 = Tsv(outputDir+"/crawl_"+level+"/dummy2.tsv")
    val dummy3 = Tsv(outputDir+"/crawl_"+level+"/dummy3.tsv")

    /**
     * Read from status.tsv and output the fetched urls
     */
    //TODO: find a way to split in one step
    val fetched = status
            .read
            .filter('status) { status: String => status == "fetched" }

    /**
     * Read from status.tsv and output the unfetched urls
     */
    val unfetched = status
            .read
            .filter('status) { status: String => status == "unfetched" }

    /**
     * get unique unfetched links by joining links and status
     */
    val unfetchedLinks = links
                .rename(('url, 'timestamp) -> ('unfetchedUrl, 'timestampX))
                .joinWithSmaller('unfetchedUrl -> 'url, fetched, joiner = new LeftJoin)
                .filter('status) {status: String => status != "fetched"}
                .project('unfetchedUrl)

    unfetchedLinks
        .write(dummy)

    /**
     * get all unfetched links
     */
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

    /**
     * foreach allUnfetched -> fetch content and write to raw and parsed
     */
    val rawTuples = allUnfetched
            .map('url -> ('timestamp, 'status, 'content, 'links)) { url: String => {
                    val fetcher = new SimpleHttpFetcher(1, userAgent)
                    val (status, content) = try {
                        val fetch = fetcher.fetch(url).getContent
                        ("fetched", new String(fetch))
                    } catch {
                        case e: Exception => ("error", "")
                    }
                    //todo: normalize url
                    val links = content match {
                        case s: String if s.length > 0 => {
                            val doc = Jsoup.parse(s)
                            doc.getElementsByTag("a").map(link => link.attr("href") ).mkString(",")
                        }
                        case _ => ""
                    }
                    (new DateTime().getMillis, status, content, links)
                }
            }

    rawTuples
        .write(raw)

    /**
     * Write outgoing links from rawTuples to next links level
     */
    val outgoingLinks = rawTuples
            .project('links)
            .flatMapTo('links -> 'link) { links : String => links.split(",")}
            .unique('link)
            .filter('link) {link: String => link != null && link != ""}
            .mapTo('link -> ('url, 'timestamp, 'referer)) { link: String => (link, new DateTime().getMillis, "referer")}

    outgoingLinks
        .write(linksOutput)

    /**
     * Parse out the content and write to parsed.tsv
     */
    val parsedTuples = rawTuples
            .map('content -> ('businessName, 'category, 'rating)) { content: String => {
                    val doc = Jsoup.parse(content)
                    def extractById(id: String) = {
                        Option(doc.getElementById(id)) match {
                            case Some(ele) => ele.text()
                            case None => ""
                        }
                    }
                    val business = extractById("business")
                    val category = extractById("category")
                    val rating = extractById("rating")

                    (business, category, rating)
                }
            }
            .discard('content, 'links, 'status)

    parsedTuples
        .write(parsed)


    /**
     * Write status of each fetch
     */
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