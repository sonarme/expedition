package com.sonar.expedition.scrawler.linkedin

import grizzled.slf4j.Logging
import org.jsoup.nodes.{Element, Document}
import org.apache.tika.metadata.{HttpHeaders, Metadata}
import com.sonar.dossier.dto.{Aliases, ServiceProfileDTO, ServiceType}
import java.net.URL
import org.apache.commons.lang.NotImplementedException
import scala.collection.JavaConversions._
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import java.util.regex.{Matcher, Pattern}
import java.io.BufferedReader
import org.apache.log4j.Logger
import com.sonar.dossier.ScalaGoodies._

/*
* Created by IntelliJ IDEA.
* User: paul
* Date: 9/5/11
* Time: 11:57 PM
*/
class LinkedinCrawlerUtils {

//
//    def extractContentsfromPageLinks(urlpass: String): String = {
//        var document: Document = null
//        var responseBodyString = ""
//        var results: String = ""
//        //todo: FIx me!!!! We should probably only have a singleton httpClient
//        val httpclient = new DefaultHttpClient()
//        val method = new HttpPost(urlpass)
//
//        // Execute the method.
//        var statusCode = httpclient.execute(method)
//        responseBodyString = method.getEntity.getContent.toString; //bytes
//        //document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get
//        document = Jsoup.parse(responseBodyString, urlpass)
//        val links: Elements = document.select("a[href]")
//        import scala.collection.JavaConversions._
//        for (link <- links) {
//            val classname: String = link.className
//            if (classname.indexOf("memName") != -1) {
//                val memurl: String = link.attr("abs:href")
//                results += memurl + "\n"
//            }
//        }
//        results
//    }

//    def extractContentsPageLinks(url: String): String = {
//        var results = ""
////        val highestpage = findhighestepageNum(url)
//        val index = url.indexOf("meetup.com") + 11
//        var groupname: String = url.substring(index, index + url.substring(index).indexOf("/"))
//        if (1 == highestpage) {
//            val tmpurl = "http://www.meetup.com/" + groupname + "/members/"
//            results += tmpurl + "\n"
//
//        } else {
//            for (i <- 0 until highestpage + 1) {
//                val tmpurl = "http://www.meetup.com/" + groupname + "/members/?offset=" + i * 20 + "&desc=1&sort=chapter_member.atime"
//                results += tmpurl + "\n"
//            }
//        }
//
//        results
//
//    }

    def checkIfProfileURL(url: String): Boolean = {
        val pattern: Pattern = Pattern.compile("http://www.linkedin.com/.*")
        val matcher: Matcher = pattern.matcher(url)
        matcher.matches()
    }

//    def doHttpUrlConnectionAction(desiredUrl: String): String = {
//        var responseBodyString = ""
//        var results: String = ""
//        var httpclient = new DefaultHttpClient()
//        var method = new HttpPost(desiredUrl)
//        var statusCode = httpclient.execute(method)
//        responseBodyString = method.getEntity.getContent.toString; //bytes
//        responseBodyString
//    }
//
//    def getResponse(reader: BufferedReader): Option[String] = {
//        // Wrap the Java result in an Option (this will become a Some or a None)
//        Option(reader.readLine().toString)
//    }
//
//    def getProfileInfo(url: String): String = {
//        // meetupid , fbid, twitter id, linkedin id, groupname_in meetup,location tuple in CSV
//        val profiledocument: Document = getPageContentsDoc(url)
//        val res: String = extractProfileMeetUpId(url) + "," + extractProfileName(profiledocument) + "," + extractProfileFBId(profiledocument) + "," + extractProfileTWId(profiledocument) + "," + extractProfileLINKEDId(profiledocument) + "," + extractProfileGrpName(url) + "," + extractProfileLocation(profiledocument)
//        res
//    }

    def getPageContentsDoc(urlpass: String): Document = {
        var profiledocument: Document = null
        var responseBodyString = ""
        var results: String = ""
        val httpclient = new DefaultHttpClient()
        val method = new HttpGet(urlpass)
        var statusCode = httpclient.execute(method)
        responseBodyString = ?(method.getEntity.getContent.toString) //bytes
        profiledocument = Jsoup.parse(responseBodyString, urlpass)
        profiledocument
    }

//    def extractProfileLinkedinId(url: String): String = {
//        if (url.indexOf("www.linkedin.com/in/").!=(-1))
//            url.substring(url.indexOf("/in/") + 4, url.length() - 1)
//        else
//            "0"
//    }

//    def extractProfileFBId(profiledocument: Document): String = {
//        var elem: Elements = profiledocument.getElementsByClass("badge-facebook-24")
//        var urlID: String = elem.attr("abs:href")
//        if (urlID.indexOf("id=").!=(-1)) {
//            urlID.substring(urlID.indexOf("id=") + 3, urlID.length())
//        } else {
//            if (urlID.indexOf("www.facebook.com").!=(-1))
//                urlID.substring(urlID.indexOf("www.facebook.com") + 17)
//            else
//                "0"
//        }
//    }




    /**
     * Callback from DossierTikaCallable so that we can extract via Jsoup
     */
     def parseJsoup(document: Document, stringUrl: String) = {
        val locationUrl = new URL(stringUrl)
        val locationPath = locationUrl.getPath
        locationPath match {

            case LinkedinCrawlerUtils.LI_PROFILE_PAGE_PATTERN(matchedProfile) => {

                val rawProfile = document.select("div#content")
                val rawProfileHtml = rawProfile.html()

                val profileImageElem = rawProfile.select("div#profile-picture>image.photo").first()
                val profileImageSrc = profileImageElem match {
                    case imageSrc: Element => {
                        imageSrc.attr("abs:src")
                    }
                    case _ => ""

                }
                val profileNameElem = rawProfile.select("span.full-name").first()
                val profileName = if (profileNameElem != null) {
                    profileNameElem.text()
                } else {
                    null
                }
                val jobTitleElem = rawProfile.select("p.headline-title").first()
                val jobTitle = if (jobTitleElem != null) {
                    jobTitleElem.text()
                } else {
                    null
                }

                val companyElem = rawProfile.select("[class*=company-profile]").first()
                val companyDescription = if (companyElem != null) {
                    companyElem.text()
                } else {
                    ""
                }

                val geoLocation = Option(rawProfile.select("span.locality").first()).map(_.text).orNull
                val profileDescriptionElem = rawProfile.select("div.content p.description").first()
                val profileDescriptionContent = if (profileDescriptionElem != null) {
                    profileDescriptionElem.html()
                } else ""

                val profileExperienceElem = rawProfile.select("div#profile-experience").first()
                val profileExperienceContent = if (profileExperienceElem != null) {
                    profileExperienceElem.html()
                } else ""

                val profileEducationElem = rawProfile.select("div#profile-education").first()
                val profileEducationContent = if (profileEducationElem != null) {
                    profileEducationElem.html()
                } else ""


                val websitesElements = rawProfile.select("li.website > a")
                val websiteIt = websitesElements.iterator()
                val websiteList = new StringBuilder().append("Websites: \n")
                while (websiteIt.hasNext) {
                    val curWebsite = websiteIt.next()

                    if (curWebsite != null) {
                        val curWebsiteElem = curWebsite.asInstanceOf[Element]
                        val websiteName = curWebsiteElem.text()
                        curWebsiteElem.attr("href") match {
                            case LinkedinCrawlerUtils.LI_WEBSITE_LINK_PATTERN(curWebsiteUrl, remainingParams) =>
                                val decoder = new java.net.URLDecoder()
                                val extractedWebsiteUrl = java.net.URLDecoder.decode(curWebsiteUrl, "utf-8")
                                websiteList.append(extractedWebsiteUrl).append("\n")
                            //addSocialNetwork(ServiceProfileDTO(ServiceType.Customwebsite, extractedWebsiteUrl, extractedWebsiteUrl))
                            case _ => LinkedinCrawlerUtils.LOGGER.error("error matching website pattern for: " + websiteName)
                        }
                    }
                }


                val publicProfileUrl = Option(rawProfile.select("a[name=webProfileURL]").first()).map(_.text).orNull

                val userId = Option(document.select("meta[name=UniqueID]").first()).map(_.attr("content")).orNull
//                if (!LinkedinCrawler.privateExtraction) {
//                    throw new NotImplementedException("Need to extract userId from JavaScript on public not-loggedin profile pages")
//                }
                  //todo readd this code below and the privateprofile part above
//                Option(rawProfile.select("a[href~=.*?twitter(\\%2E|\\.)com.*?]").first()).map {
//                    twitterElem =>
//                        java.net.URLDecoder.decode(twitterElem.attr("href"), "utf-8") match {
//                            case LinkedinCrawlerUtils.LI_TWITTER_URL_PATTERN(twitterId, additonalParams) => addSocialNetwork(ServiceProfileDTO(ServiceType.twitter, twitterId, "http://twitter.com/" + twitterId))
//                            case _ =>
//                        }
//                }


                val linkedInServiceProfile = ServiceProfileDTO(ServiceType.linkedin, userId, stringUrl)



                //                linkedInServiceProfile.geoLocation = geoLocation
                //                linkedInServiceProfile.company = companyDescription
                linkedInServiceProfile.setFullName(profileName)
                linkedInServiceProfile.setImageUrl(profileImageSrc)
                //                linkedInServiceProfile.jobDescription = jobTitle
                //                linkedInServiceProfile.resume = profileExperienceContent
                linkedInServiceProfile.setBio(profileEducationContent)
                linkedInServiceProfile.setRawProfile(rawProfileHtml)
                //                linkedInServiceProfile.serviceProfileDetails = websiteList.toString()
                //                linkedInServiceProfile. = profileDescriptionContent
                //                linkedInServiceProfile.sourceProfileUrl = location
                //                linkedInServiceProfile.sourceServiceType = ServiceType.linkedin
                new URL(publicProfileUrl).getPath match {
                    case LinkedinCrawlerUtils.LI_ALIAS_PATTERN(alias) => linkedInServiceProfile.setAliases(alias.asInstanceOf[Aliases])
                    case _ =>
                }

                //todo: add websites as other services field
                 //todo: enable this line below again
//                addSocialNetwork(linkedInServiceProfile)

            }
            case _ => {
               LinkedinCrawlerUtils.LOGGER.error("error matching URL so this is probably not a member profile page — ok to ignore: '" + stringUrl + "'")
            }
        }

    }
}

object LinkedinCrawlerUtils {

    val LI_ALIAS_PATTERN = """/in/([\w\d%-]+)""".r
    val LI_PROFILE_PAGE_PATTERN =  """(/in/[\w\d%-]+|/pub/[\w\d%-]+/[\w\d]{1,4}/[\w\d]{1,4}/[\w\d]{1,4})/?""".r
    val LI_FULL_PROFILE_LINK_PATTERN = """.*?/profile/view\?id=([\d]+)&.*?""".r

    val LI_TWITTER_URL_PATTERN = """.*?twitter\.com/([\w\d\-]+)(&.*|$)""".r

    val LI_WEBSITE_LINK_PATTERN = """/redirect\?url=([\w\d\%\-]+)(&.*|$)""".r

    //todo:enable Logger
    val LOGGER: Logger = Logger.getLogger(classOf[LinkedinCrawl])

}

