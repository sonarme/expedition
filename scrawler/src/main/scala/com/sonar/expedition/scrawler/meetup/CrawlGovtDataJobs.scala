package com.sonar.expedition.scrawler.meetup

import com.twitter.scalding.{TextLine, Args}
import org.apache.http.client.utils.URLEncodedUtils
import java.net.URI
import util.matching.Regex
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.sonar.expedition.scrawler.jobs.Job

class CrawlGovtDataJobs(args: Args) extends Job(args) {

    val input = args.getOrElse("input", "/tmp/govtdata")

    var links1 = (TextLine(args.apply("input")).read
            .project('line)
            .flatMap('line -> 'links) {
        line: String => importLinks(line)
    }.filter('links) {
        joblinks: String => ((joblinks.contains("/ooh") && joblinks.endsWith(".htm")) || joblinks.contains("home") || joblinks.contains("index") || joblinks.contains("faq") || joblinks.contains("site-map"))
    }.project('links)
            .flatMapTo('links ->('salary, 'contents)) {
        fields: (String) =>
            val (links) = fields
            val (salary, keywords) = extractContentsPageLinks(links)
            val splitvalue = keywords.mkString.split(" ")
            for (key <- splitvalue) yield (salary.mkString, key.mkString)


    }
            ).project('salary, 'contents).write(TextLine("/tmp/datajobs"))

    def importLinks(url: String) = {
        val document = ScrawlerUtils.getPageContentsDoc(url)
        val pager = document.select( """a[href]""")
        for (i <- 0 to pager.size() - 1)
        yield pager.get(i).attr("abs:href")

    }

    def extractContentsPageLinks(url: String): (Option[String], Option[String]) = {
        try {
            Thread.sleep(200)
            val document = ScrawlerUtils.getPageContentsDoc(url)
            val line = document.body().toString
            if (line.contains("per year")) {
                val res = line.substring(line.indexOf("per year") - 9, line.indexOf("per year"))
                val words = document.select("meta").attr("abs:content");
                (Some(res), Some(words))
            } else {
                (None, None)
            }
        } catch {
            case e => Thread.sleep(2000); extractContentsPageLinks(url);
        }
    }
}

