package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding.{TextLine, Tsv, Job, Args}
import com.sonar.expedition.scrawler.util.Tuples
import crawlercommons.sitemaps.{SiteMap, SiteMapIndex, SiteMapParser}
import java.net.URL
import org.springframework.util.FileCopyUtils
import java.io.{FileReader, FileInputStream, InputStreamReader}
import org.apache.commons.lang.StringUtils
import java.util.Date

/**
 * Write the links in a sitemap into a Tsv file specified by the output
 * @param args
 */
class SitemapLinksJob(args: Args) extends Job(args) with SitemapFilter {
    val sitemap = args("sitemap")
    val output = args("output")

    val linksOutTsv = Tsv(output, Tuples.Crawler.Links)
    val dummy = TextLine(getClass.getResource("/datafiles/dummy.txt").getFile)

    val links = dummy
        .read
        .flatMapTo('line -> Tuples.Crawler.Links) {
            line: String => {
                //ported from java code

                val siteMapParser = new SiteMapParser
                val url = new URL(sitemap)
                val connection = url.openConnection()
                var siteMapContent = StringUtils.stripToEmpty(FileCopyUtils.copyToString(new InputStreamReader(connection.getInputStream)))
                val contentType = connection.getContentType
                val abstractSiteMapIndex = siteMapParser.parseSiteMap(contentType, siteMapContent.getBytes, url)

                val tuples = abstractSiteMapIndex match {
                    case siteMapIndex: SiteMapIndex if siteMapIndex.isIndex => {
                        siteMapIndex.getSitemaps.flatMap(abstractSiteMap => {
                            abstractSiteMap match {
                                case sm: SiteMap if (!sm.isIndex && isSiteMapIncluded(sm.getUrl.toExternalForm)) => {
                                    val curSiteMapConnection = sm.getUrl.openConnection()
                                    val curSiteMapContent = FileCopyUtils.copyToByteArray(curSiteMapConnection.getInputStream)
                                    val curSiteMap = siteMapParser.parseSiteMap("application/gzipped", curSiteMapContent, sm.getUrl)
                                    curSiteMap match {
                                        case s: SiteMap => s.getSiteMapUrls.map(siteMapUrl => {
                                            Some((siteMapUrl.getUrl.toExternalForm, new Date().getTime.toString, ""))
                                        })
                                        case _ => None
                                    }
                                }
                                case _ => None
                            }
                        })
                    }
                    case _ => List.empty[Option[(String, String, String)]]
                }
                tuples.flatten.take(10)
            }
        }

    links
        .write(linksOutTsv)
}