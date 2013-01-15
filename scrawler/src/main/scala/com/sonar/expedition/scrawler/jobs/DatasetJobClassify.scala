package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Job, Args, TextLine}
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import org.jsoup.nodes.Document
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpPost
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import java.io.{InputStreamReader, BufferedReader}

import DatasetJobClassify._
import scala.collection.JavaConversions._

// not finished, crawling gov data for job classification
class DatasetJobClassify(args: Args) extends DefaultJob(args) {

    val jobpipe = TextLine("/tmp/links").read.project('line)
            .mapTo('line -> 'link) {
        line: String =>
            line
    }
            .flatMapTo('link -> 'jobs) {
        fields: String =>
            val (link) = fields

            getJobs(link)

    }.mapTo('jobs ->('jobname, 'link)) {
        fields: String =>
            val (jobs) = fields
            val jobssplt = jobs.split("\t")
            (jobssplt(1), jobssplt(0))
    }
            .mapTo(('jobname, 'link) ->('job, 'desc)) {
        fields: (String, String) =>
            val (jobs, link) = fields
            val desc = getDesc(link)
            (jobs, desc)

    }
            .write(TextLine("/tmp/jobsdata1"))

    def getDesc(urlpass: String): String = {
        val httpclient = new DefaultHttpClient()
        val method = new HttpPost(urlpass)
        // Execute the method.
        val statusCode = httpclient.execute(method)
        val responseIS = statusCode.getEntity().getContent()
        val reader = new BufferedReader(new InputStreamReader(responseIS))
        val responseBodyString = Stream.continually(reader.readLine()).takeWhile(_ ne null).mkString
        val document = Jsoup.parse(responseBodyString, urlpass)
        var _start = responseBodyString.indexOf("Begin Main Content");
        //println(_start)
        var _end = responseBodyString.substring(_start).indexOf("</p>", 0);
        //println(_end)
        var res = responseBodyString.substring(_start + 35, _start + _end).replaceAll("\\n", "")
        //println(res)
        res
    }

    def getJobs(urlpass: String) = {

        //todo: fix me!!!! We should probably only have a singleton httpClient
        val httpclient = new DefaultHttpClient()
        val method = new HttpPost(urlpass)
        // Execute the method.
        val statusCode = httpclient.execute(method)
        val responseIS = statusCode.getEntity().getContent()
        val reader = new BufferedReader(new InputStreamReader(responseIS))
        val responseBodyString = Stream.continually(reader.readLine()).takeWhile(_ ne null).mkString
        val document = Jsoup.parse(responseBodyString, urlpass)
        val links: Elements = document.select("a[HREF]")

        for (link <- links;
             absLink = link.attr("abs:href");
             memurl <- absLink match {
                 case jobs(jobLink) => Some(absLink + "\t" + link.text() + "\n")
                 case _ => None
             }) yield memurl
    }

}

object DatasetJobClassify {
    val jobs = """http://www.bls.gov/oes/current/oes(\d+).htm""".r

}
