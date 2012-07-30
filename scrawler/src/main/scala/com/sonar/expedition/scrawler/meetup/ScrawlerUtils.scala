package com.sonar.expedition.scrawler.meetup


import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.net.{URL, URI}

import org.apache.http.client.utils.URLEncodedUtils
import java.io._;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Document;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.apache.http.util.EntityUtils
import scala.collection.JavaConversions._
import util.matching.Regex
import com.sonar.dossier.dto.{ServiceProfileLink, ServiceType}


object ScrawlerUtils {

    val meetupPageSize = 20
    val groupname = """http://www.meetup.com/([a-zA-Z\d\-]+)/members/(\d+)/""".r
    val groupnamefromurl = """http://www.meetup.com/([a-zA-Z\d\-]+)/members/""".r

    val extractors = List(ServiceType.facebook ->("badge-facebook-24", """http://www.facebook.com/(.+)""".r),
        ServiceType.twitter ->("badge-twitter-24", """http://twitter.com/(.+)""".r),
        ServiceType.linkedin ->("badge-linkedin-24", """http://www.linkedin.com/(.+)""".r))
    //val meetupid=         """http://www.meetup.com/([a-zA-Z\d\-]+)/members/(\d+)/""".r
    //val profilegroupname= """http://www.meetup.com/([a-zA-Z\d\-]+)/members/(\d+)/""".r

    def extractContentsfromPageLinks(urlpass: String) = {
        val document = getPageContentsDoc(urlpass)
        val links = document.select("a[class=memName]")
        links flatMap {
            link => Option(link.attr("abs:href"))
        }
    }

    def checkIfProfileURL(url: String): Boolean = {
        val pattern: Pattern = Pattern.compile(".*\\d{7,8}/");
        val matcher: Matcher = pattern.matcher(url);
        matcher.matches();
    }

    def extractContentsPageLinks(url: String) =
        url match {
            case groupnamefromurl(gname) => {
                val highestpage = findLastMeetupMemberPage(url)

                for (i <- 0 to highestpage)
                yield "http://www.meetup.com/" + gname + "/members/?offset=" + (i * meetupPageSize) + "&desc=1&sort=chapter_member.atime"
            }
            case _ => List.empty[String]
        }

    def findLastMeetupMemberPage(url: String) = {

        val document = getPageContentsDoc(url)
        val pager = document.select( """ul[class=D_pager border-none]""")
        val relPagers = pager.select( """li[class=relative_page]""")
        if (relPagers.isEmpty) 0
        else {
            val lastPager = relPagers.last()
            val lastPagerUrl = lastPager.select("a").attr("href")
            val nameValuePairs = URLEncodedUtils.parse(new URI(lastPagerUrl), null)
            nameValuePairs.find(_.getName == "offset").map(_.getValue.toInt / meetupPageSize).getOrElse(0)
        }

    }

    def getResponse(reader: BufferedReader): Option[String] = {
        Option(reader.readLine().toString);
    }


    def getProfileInfo(url: String): String = {
        // meetupid , fbid, twitter id, linkedin id, groupname_in meetup,location tuple in CSV
        var profiledocument = getPageContentsDoc(url)
        val serviceProfiles = for ((serviceType, (cssClass, regex)) <- extractors;
                                   profileId = extractProfileId(regex, profiledocument, cssClass) getOrElse "")
        yield profileId

        val (name, id) = extractProfileMeetUpId(url) getOrElse("", "")
        val resultList: List[String] = List(id, extractProfileName(profiledocument) getOrElse "") ++ serviceProfiles ++ List(name, extractProfileLocation(profiledocument).productIterator.toList.mkString("(", ",", ")"))
        resultList.mkString(",")

    }

    def getPageContentsDoc(urlpass: String): Document = {
        val httpclient = new DefaultHttpClient
        val method = new HttpPost(urlpass)
        method.setHeader("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2")
        val statusCode = httpclient.execute(method)
        val responseBodyString = EntityUtils.toString(statusCode.getEntity)
        Jsoup.parse(responseBodyString, urlpass)

    }

    def extractProfileName(profiledocument: Document) = {

        val span: Elements = profiledocument.select("span[class=memName fn]");
        Option(span(0).text())

    }

    def extractProfileMeetUpId(url: String) = {
        url match {
            case groupname(name, id) => Some((name, id))
            case _ => None
        }

    }

    def extractProfileId(r: Regex, doc: Document, cssClass: String) = {
        val elem: Elements = doc.getElementsByClass(cssClass)
        val urlID: String = elem.attr("abs:href")
        urlID match {
            case r(id) => Some(id)
            case _ => None
        }
    }

    def extractProfileLocation(profiledocument: Document): (String, String, String) = {
        (profiledocument.getElementsByClass("locality").text(), profiledocument.getElementsByClass("region").text(), profiledocument.getElementsByClass("displaynone country-name").text());

    }
}
