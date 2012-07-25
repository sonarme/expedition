package com.sonar.expedition.scrawler.meetup

import org.apache.http.client.HttpClient
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
import collection.JavaConversions._
import com.sun.deploy.net.HttpResponse
import org.springframework.http.{HttpMethod, HttpEntity}

import org.apache.http.util.EntityUtils
import org.unitils.thirdparty.org.apache.commons.io.IOUtils
;


object ScrawlerUtils {
    val meetupPageSize = 20

    def extractContentsfromPageLinks(urlpass: String): String = {
        var document: Document = null
        var responseBodyString = ""
        var results: String = ""
        //todo: Fix me!!!! We should probably only have a singleton httpClient
        var httpclient = new DefaultHttpClient();
        var method = new HttpPost(urlpass);
        var statusCode = httpclient.execute(method);
        responseBodyString = method.getEntity.getContent.toString; //bytes
        //document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get
        document = Jsoup.parse(responseBodyString, urlpass);
        val links: Elements = document.select("a[href]")
        import scala.collection.JavaConversions._
        for (link <- links) {
            val classname: String = link.className
            if (classname.indexOf("memName") != -1) {
                val memurl: String = link.attr("abs:href")
                results += memurl + "\n"
            }
        }
        results
    }

    def checkIfProfileURL(url: String): Boolean = {
        val pattern: Pattern = Pattern.compile(".*\\d{7,8}/");
        val matcher: Matcher = pattern.matcher(url);
        matcher.matches();
    }

    def extractContentsPageLinks(url: String): String = {
        var results = ""
        val highestpage = findLastMeetupMemberPage(url);
        val index = url.indexOf("meetup.com") + 11;
        var groupname: String = url.substring(index, index + url.substring(index).indexOf("/"));
        if (1 == highestpage) {
            val tmpurl = "http://www.meetup.com/" + groupname + "/members/";
            results += tmpurl + "\n";

        } else {
            for (i <- 0 until highestpage + 1) {
                val tmpurl = "http://www.meetup.com/" + groupname + "/members/?offset=" + i * 20 + "&desc=1&sort=chapter_member.atime";
                results += tmpurl + "\n";
            }
        }

        results

    }

    def findLastMeetupMemberPage(url: String): Int = {

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


    def doHttpUrlConnectionAction(desiredUrl: String): String = {
        val httpclient = new DefaultHttpClient()
        val method = new HttpPost(desiredUrl)
        val statusCode = httpclient.execute(method)
        val responseIS = statusCode.getEntity().getContent()
        val reader = new BufferedReader(new InputStreamReader(responseIS))
        val responseBodyString = Stream.continually(reader.readLine()).takeWhile(_ ne null).mkString
        println(responseBodyString)
        responseBodyString

    }


    def getResponse(reader: BufferedReader): Option[String] = {
        Option(reader.readLine().toString);
    }

    def getProfileInfo(url: String): String = {
        // meetupid , fbid, twitter id, linkedin id, groupname_in meetup,location tuple in CSV
        var profiledocument: Document = getPageContentsDoc(url);
        val res: String = extractProfileMeetUpId(url) + "," + extractProfileName(profiledocument) + "," + extractProfileFBId(profiledocument) + "," + extractProfileTWId(profiledocument) + "," + extractProfileLINKEDId(profiledocument) + "," + extractProfileGrpName(url) + "," + extractProfileLocation(profiledocument)
        res
    }

    def getPageContentsDoc(urlpass: String): Document = {
        val httpclient = new DefaultHttpClient

        val method = new HttpPost(urlpass)
        method.setHeader("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2")
        val statusCode = httpclient.execute(method)
        val responseBodyString = EntityUtils.toString(statusCode.getEntity)
        Jsoup.parse(responseBodyString, urlpass)

    }

    def extractProfileName(profiledocument: Document): String = {
        val span: Elements = profiledocument.select("span");
        var name: String = "";
        for (i <- 0 until span.size()) {
            if (span.get(i).className().trim.equalsIgnoreCase("memName fn"))
                name = span.get(i).text();
        }
        name;
    }

    def extractProfileMeetUpId(url: String): String = {
        if (url.indexOf("members/").!=(-1))
            url.substring(url.indexOf("members/") + 8, url.length() - 1);
        else
            "0"
    }

    def extractProfileGrpName(url: String): String = {
        if (url.indexOf("com/").!=(-1) && url.indexOf("/members/").!=(-1))
            url.substring(url.indexOf("com/") + 4, url.indexOf("/members/"));
        else
            ""
    }

    def extractProfileFBId(profiledocument: Document): String = {
        var elem: Elements = profiledocument.getElementsByClass("badge-facebook-24");
        var urlID: String = elem.attr("abs:href");
        if (urlID.indexOf("id=").!=(-1)) {
            urlID.substring(urlID.indexOf("id=") + 3, urlID.length());
        } else {
            if (urlID.indexOf("www.facebook.com").!=(-1))
                urlID.substring(urlID.indexOf("www.facebook.com") + 17);
            else
                "0"
        }
    }

    def extractProfileTWId(profiledocument: Document): String = {

        var elem: Elements = profiledocument.getElementsByClass("badge-twitter-24");
        var urlID: String = elem.attr("abs:href");
        if (urlID.indexOf("twitter.com").!=(-1))
            urlID.substring(urlID.indexOf("twitter.com") + 12, urlID.length() - 1);
        else "0"
    }


    def extractProfileLINKEDId(profiledocument: Document): String = {
        var elem: Elements = profiledocument.getElementsByClass("badge-linkedin-24");
        var urlID: String = elem.attr("abs:href");
        if (urlID.indexOf("linkedin.com").!=(-1))
            urlID.substring(urlID.indexOf("linkedin.com") + 16, urlID.length());
        else "0"

    }

    def extractProfileLocation(profiledocument: Document): String = {
        var locality: String = ""
        var region: String = ""
        var elem: Elements = profiledocument.getElementsByClass("locality");
        locality = elem.text();
        elem = profiledocument.getElementsByClass("region");
        if (elem.text().split(" ").isDefinedAt(0)) {
            region = elem.text().split(" ")(0);
            locality + " " + region;
        } else ""
    }
}
