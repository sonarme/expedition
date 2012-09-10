package com.sonar.expedition.scrawler.crawler

import java.net.URL
import org.apache.commons.validator.routines.UrlValidator
import org.jsoup.Jsoup
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import com.sonar.expedition.scrawler.publicprofile.PublicProfileCrawlerUtils
import util.Random
import org.apache.http.util.EntityUtils
import collection.JavaConversions._

class Crawler {
    def fetchToTuple(url: String, domains: String = ""): Tuple3[String, String, Iterable[String]] = {
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