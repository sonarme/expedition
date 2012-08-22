package com.sonar.expedition.scrawler.meetup

import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.HttpStatus
import org.apache.http.impl.client.DefaultHttpClient
import io.Source

//import org.apache.commons.httpclient.{HttpStatus, HttpClient}

import java.io._
import org.jsoup.select.Elements
import org.jsoup.nodes.Document
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.jsoup.Jsoup


object ScrawlerUtils {

    def extractContentsfromPageLinks(urlpass: String): String = {
        var document: Document = null
        var responseBodyString = ""
        var results: String = ""
        //todo: FIx me!!!! We should probably only have a singleton httpClient
        val httpclient = new DefaultHttpClient()
        val method = new HttpPost(urlpass)

        // Execute the method.
        var statusCode = httpclient.execute(method)
        responseBodyString = method.getEntity.getContent.toString //bytes
        //document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1 WOW64 rv:5.0) Gecko/20100101 Firefox/5.0").get
        document = Jsoup.parse(responseBodyString, urlpass)
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
        val pattern: Pattern = Pattern.compile(".*\\d{7,8}/")
        val matcher: Matcher = pattern.matcher(url)
        matcher.matches()
    }

    def extractContentsPageLinks(url: String): String = {
        val highestpage = findhighestepageNum(url)
        val index = url.indexOf("meetup.com") + 11
        var groupname: String = url.substring(index, index + url.substring(index).indexOf("/"))
        if (1 == highestpage) {
            "http://www.meetup.com/" + groupname + "/members/"
        } else {
            val links = for (i <- 0 until highestpage + 1) yield "http://www.meetup.com/" + groupname + "/members/?offset=" + i * 20 + "&desc=1&sort=chapter_member.atime"
            links.mkString("\n")
        }
    }

    def findhighestepageNum(url: String): Int = {
        var highestPage = 1
        var tmppage = 1
        var page = 1
        var memurl = doHttpUrlConnectionAction(url).split("relative_page")
        if (memurl.isDefinedAt(2)) {
            if (memurl(2).indexOf("?offset=") != -(1)) {
                val tmpindex = memurl(2).indexOf("?offset=") + 8
                val innertmpind = memurl(2).substring(tmpindex).indexOf("&")
                tmppage = Integer.parseInt(memurl(2).substring(tmpindex, tmpindex + innertmpind))
                if (tmppage > highestPage)
                    highestPage = tmppage
            }
            val page: Int = highestPage match {
                case 1 => 1
                case _ => highestPage / 20
            }
            println(page)
            page
        } else {
            1
        }


    }


    def doHttpUrlConnectionAction(desiredUrl: String): String = {
        var httpclient = new DefaultHttpClient()
        var method = new HttpGet(desiredUrl)
        var contentStream = httpclient.execute(method).getEntity.getContent
        Source.fromInputStream(contentStream).mkString
    }

    def getResponse(reader: BufferedReader): Option[String] = {
        // Wrap the Java result in an Option (this will become a Some or a None)
        Option(reader.readLine().toString)
    }

    def getProfileInfo(url: String): String = {
        // meetupid , fbid, twitter id, linkedin id, groupname_in meetup,location tuple in CSV
        var profiledocument: Document = getPageContentsDoc(url)
        val res: String = extractProfileMeetUpId(url) + "," + extractProfileName(profiledocument) + "," + extractProfileFBId(profiledocument) + "," + extractProfileTWId(profiledocument) + "," + extractProfileLINKEDId(profiledocument) + "," + extractProfileGrpName(url) + "," + extractProfileLocation(profiledocument)
        res
    }

    def getPageContentsDoc(urlpass: String): Document = {
        var profiledocument: Document = null
        var responseBodyString = ""
        var results: String = ""
        var httpclient = new DefaultHttpClient()
        var method = new HttpPost(urlpass)
        var statusCode = httpclient.execute(method)
        responseBodyString = method.getEntity.getContent.toString //bytes
        profiledocument = Jsoup.parse(responseBodyString, urlpass)
        profiledocument
    }

    def extractProfileName(profiledocument: Document): String = {
        val span: Elements = profiledocument.select("span")
        var name: String = ""
        for (i <- 0 until span.size()) {
            if (span.get(i).className().trim.equalsIgnoreCase("memName fn"))
                name = span.get(i).text()
        }
        name
    }

    def extractProfileMeetUpId(url: String): String = {
        if (url.indexOf("members/").!=(-1))
            url.substring(url.indexOf("members/") + 8, url.length() - 1)
        else
            "0"
    }

    def extractProfileGrpName(url: String): String = {
        if (url.indexOf("com/").!=(-1) && url.indexOf("/members/").!=(-1))
            url.substring(url.indexOf("com/") + 4, url.indexOf("/members/"))
        else
            ""
    }

    def extractProfileFBId(profiledocument: Document): String = {
        var elem: Elements = profiledocument.getElementsByClass("badge-facebook-24")
        var urlID: String = elem.attr("abs:href")
        if (urlID.indexOf("id=").!=(-1)) {
            urlID.substring(urlID.indexOf("id=") + 3, urlID.length())
        } else {
            if (urlID.indexOf("www.facebook.com").!=(-1))
                urlID.substring(urlID.indexOf("www.facebook.com") + 17)
            else
                "0"
        }
    }

    def extractProfileTWId(profiledocument: Document): String = {

        var elem: Elements = profiledocument.getElementsByClass("badge-twitter-24")
        var urlID: String = elem.attr("abs:href")
        if (urlID.indexOf("twitter.com").!=(-1))
            urlID.substring(urlID.indexOf("twitter.com") + 12, urlID.length() - 1)
        else "0"
    }


    def extractProfileLINKEDId(profiledocument: Document): String = {
        var elem: Elements = profiledocument.getElementsByClass("badge-linkedin-24")
        var urlID: String = elem.attr("abs:href")
        if (urlID.indexOf("linkedin.com").!=(-1))
            urlID.substring(urlID.indexOf("linkedin.com") + 16, urlID.length())
        else "0"

    }

    def extractProfileLocation(profiledocument: Document): String = {
        var locality: String = ""
        var region: String = ""
        var elem: Elements = profiledocument.getElementsByClass("locality")
        locality = elem.text()
        elem = profiledocument.getElementsByClass("region")
        if (elem.text().split(" ").isDefinedAt(0)) {
            region = elem.text().split(" ")(0)
            locality + " " + region
        } else ""
    }
}
