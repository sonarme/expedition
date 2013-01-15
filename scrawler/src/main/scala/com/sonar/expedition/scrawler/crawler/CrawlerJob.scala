package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding._
import collection.JavaConversions._
import org.joda.time.DateTime
import cascading.pipe.joiner.{LeftJoin, OuterJoin}
import com.twitter.scalding.Tsv
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.jobs.DefaultJob

class CrawlerJob(args: Args) extends DefaultJob(args) with Fetcher with ParseFilter {

    val level: Int = args("level").toInt
    val levelUp: Int = level + 1
    val srcDir = args("srcDir")
    val domains = args("domains")

    val links = Tsv(srcDir + "/crawl_" + level + "/linksTsv", Tuples.Crawler.Links)
    val linksOutTsv = Tsv(srcDir + "/crawl_" + levelUp + "/linksTsv", Tuples.Crawler.Links)
    val status = if (level == 0) Tsv(getClass.getResource("/datafiles/dummyTsv.tsv").getFile, Tuples.Crawler.Status) else Tsv(srcDir + "/crawl_" + level + "/statusTsv", Tuples.Crawler.Status)
    val statusOutTsv = Tsv(srcDir + "/crawl_" + levelUp + "/statusTsv", Tuples.Crawler.Status)
    val rawTsv = Tsv(srcDir + "/crawl_" + level + "/rawTsv", Tuples.Crawler.Raw)
    val rawSequence = SequenceFile(srcDir + "/crawl_" + level + "/rawSequence", Tuples.Crawler.Raw)


    //Read from statusTsv and output the fetched urls
    val fetched = status
            .read
            .discard('timestamp)
            .filter('status) {
        status: String => status == "fetched"
    }


    //Read from statusTsv and output the unfetched urls
    val unfetched = status
            .read
            .discard('timestamp)
            .filter('status) {
        status: String => status == "unfetched"
    }


    //get unique unfetched links by joining links and status
    val unfetchedLinks = links
            .read
            .rename('url -> 'unfetchedUrl)
            .joinWithSmaller('unfetchedUrl -> 'url, fetched, joiner = new LeftJoin)
            .filter('status) {
        status: String => status != "fetched"
    }
            .project('unfetchedUrl, 'timestamp)


    //get all unfetched links
    val allUnfetched = unfetchedLinks
            .joinWithSmaller('unfetchedUrl -> 'url, unfetched, joiner = new OuterJoin)
            .mapTo(('unfetchedUrl, 'url, 'timestamp) ->('url, 'timestamp)) {
        x: (String, String, Long) =>
            val (url, unfetchedUrl, timestamp) = x
            if (url != null)
                (url, timestamp)
            else
                (unfetchedUrl, timestamp)
    }

    //foreach allUnfetched -> fetch content and write to raw
    val rawTuples = allUnfetched
            .map('url ->('status, 'content, 'links)) {
        url: String => {
            fetchStatusContentAndLinks(url, domains)
        }
    }


    rawTuples
            .write(rawSequence)


    //Write outgoing links from rawTuples to next links level
    val outgoingLinks = rawTuples
            .flatMapTo('links -> 'link) {
        links: Iterable[String] => links.filter(link => link != null && link != "")
    }
            .unique('link)
            .mapTo('link -> Tuples.Crawler.Links) {
        link: String => (link, new DateTime().getMillis, "referer")
    }

    outgoingLinks
            .write(linksOutTsv)


    //Write status of each fetch
    val statusOut = rawTuples
            .mapTo(('url, 'status, 'timestamp) -> Tuples.Crawler.Status) {
        x: (String, String, Long) => {
            val (url, status, timestamp) = x
            (url, status, timestamp, 1, level)
        }
    }

    statusOut
            .write(statusOutTsv)

}
