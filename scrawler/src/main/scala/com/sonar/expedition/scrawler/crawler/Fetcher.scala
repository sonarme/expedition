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
import Fetcher._

trait Fetcher {
    def fetchStatusContentAndLinks(url: String, domains: String = ""): Tuple3[String, String, Iterable[String]] = {
        val (status, content) = fetchStatusAndContent(url)
        status match {
            case Fetched => {
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
            case Error => (status, content, List.empty[String])
        }
    }

    def fetchStatusAndContent(url: String): Tuple2[String, String] = {
        try {
            val content = fetchContent(url)
            if (!isError(url, content))
                (Fetched, content)
            else
                (Error, "")
        } catch {
            case e: Exception => (Error, "")
        }
    }

    def fetchContent(url: String): String = {
        val httpclient = new DefaultHttpClient
        val method = new HttpGet(url)
        method.setHeader("User-Agent", PublicProfileCrawlerUtils.USER_AGENT_LIST(Random.nextInt((PublicProfileCrawlerUtils.USER_AGENT_LIST.size) - 1)))
        try {
            println("fetching: " + url)
            val rnd = new Random()
            Thread.sleep(SleepRange(rnd.nextInt(SleepRange length)))
            val response = httpclient.execute(method)
            EntityUtils.toString(response.getEntity)
        } catch {
            case e: Exception => throw new RuntimeException("unable to fetch content for url: " + url)
        }
    }

    /**
     * Determine if the content fetched from a url is error
     * @param url
     * @param content
     * @return
     */
    private def isError(url: String, content: String): Boolean = {
        url match {
            case u: String if u.contains(Sites.Yelp) => content.contains("Sorry, you're not allowed to access this page")
            case _ => false
        }
    }
}

object Fetcher {
    val Fetched = "fetched"
    val Error = "error"
    val SleepRange = 3000 to 10000 // randomly sleep btw 3s to 10s
}