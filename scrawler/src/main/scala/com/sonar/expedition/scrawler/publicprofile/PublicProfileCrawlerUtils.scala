//package com.sonar.expedition.scrawler.publicprofile
//
//import org.jsoup.nodes.{Element, Document}
//import com.sonar.dossier.dto._
//import java.net.URL
//import scala.collection.JavaConversions._
//import org.apache.http.impl.client.DefaultHttpClient
//import org.apache.http.client.methods.{HttpGet, HttpPost}
//import org.jsoup.Jsoup
//import java.util.regex.{Matcher, Pattern}
//import org.apache.log4j.Logger
//import com.sonar.dossier.ScalaGoodies._
//import org.apache.http.util.EntityUtils
//import com.sonar.dossier.dto.ServiceProfileDTO
//import util.Random
//
//class PublicProfileCrawlerUtils {
//
//    def checkIfLIProfileURL(url: String): Boolean = {
//        val pattern: Pattern = Pattern.compile("http://www.linkedin.com/.*")
//        val matcher: Matcher = pattern.matcher(url)
//        matcher.matches()
//    }
//
//    def getPageContentsDoc(urlpass: String): Document = {
//        val httpclient = new DefaultHttpClient
//        val method = new HttpPost(urlpass)
//        method.setHeader("User-Agent", PublicProfileCrawlerUtils.USER_AGENT_LIST(Random.nextInt((PublicProfileCrawlerUtils.USER_AGENT_LIST.size) - 1)))
//        val statusCode = httpclient.execute(method)
//        val responseBodyString = EntityUtils.toString(statusCode.getEntity)
//        Jsoup.parse(responseBodyString, urlpass)
//
//    }
//
//    def getFBPageContentsDoc(urlpass: String): Document = {
//        var doc = Option(new Document(urlpass))
//        if (urlpass.matches( """https://www.facebook.com/.*""")) {
//            val httpclient = new DefaultHttpClient
//            val method = new HttpPost(urlpass)
//            method.setHeader("User-Agent", PublicProfileCrawlerUtils.USER_AGENT_LIST(Random.nextInt((PublicProfileCrawlerUtils.USER_AGENT_LIST.size) - 1)))
//            val statusCode = httpclient.execute(method)
//            val responseBodyString = EntityUtils.toString(statusCode.getEntity)
//            val commentedDoc = Jsoup.parse(responseBodyString, urlpass)
//            var uncommentedHtml = "<!DOCTYPE html>\n<html lang=\"en\" id=\"facebook\" class=\"no_js\">\n<body>\n"
//
//            val rawProfile = commentedDoc.select("div#content")
//            val nodes = commentedDoc.select("code").iterator()
//            var exists = false
//            //        || (rawProfile.select("[class*=uiHeaderImage img sp_192626 sx_1c82ff]").parents().first().text().contains("This content is currently unavailable"))
//            //        if the above condition is met that means that there is a profile with the given url, but it is not public
//            if ((rawProfile.select("[class*=uiHeaderImage img sp_192626 sx_1c82ff]").isEmpty)) {
//                exists = true
//            }
//
//            if (exists) {
//                for (node <- nodes) {
//                    uncommentedHtml += node.select(".hidden_elem").first().childNode(0).attr("comment")
//                }
//            }
//            uncommentedHtml += "\n</body></html>"
//            doc = Option(Jsoup.parse(uncommentedHtml, urlpass))
//        }
//        else {
//            PublicProfileCrawlerUtils.LOGGER.error("this URL is not a valid facebook profile: '" + urlpass + "'")
//        }
//        doc.getOrElse(new Document(urlpass))
//
//
//    }
//
//    def parseFacebook(document: Document, stringUrl: String): ServiceProfileDTO = {
//        val locationUrl = new URL(stringUrl)
//        val locationPath = locationUrl.getPath
//        val output = new ServiceProfileDTO
//
//        if (locationPath.matches( """/[\w\d\.]+""")) {
//            var imageUrl = document.select("img[class*=scaledImageFitWidth img]").text()
//            if (imageUrl.isEmpty) {
//                imageUrl = document.select("img").attr("src")
//            }
//            var name = document.select("a[itemprop=name]").text()
//            if (name.isEmpty) {
//                name = document.select("img").attr("alt")
//            }
//            val likesElements = document.select("div[class*=uiCollapsedList uiCollapsedListHidden uiCollapsedListNoSeparate pagesListData]").select("a[href]").iterator()
//            val justForSize = document.select("div[class*=uiCollapsedList uiCollapsedListHidden uiCollapsedListNoSeparate pagesListData]").select("a[href]").iterator().size
//            val like = new Array[UserLike](justForSize)
//            if (justForSize > 0) {
//                var i = 0
//                for (likes <- likesElements) {
//                    like(i) = new UserLike()
//                    like(i).setHeader(likes.text())
//                    like(i).setKey(likes.attr("href"))
//                    i += 1
//                }
//            }
//
//            var userId = ""
//            locationPath match {
//                case PublicProfileCrawlerUtils.GET_FB_USERID(userid) => userId = userid
//                case _ => userId = "can't extract user ID:" + locationPath
//            }
//
//            output.setServiceType(ServiceType.facebook)
//            output.setUserId(userId)
//            output.setFullName(name)
//            output.setImageUrl(imageUrl)
//            output.setLike(like.toList)
//            output.setNotPublic(false)
//            output.setUrl(stringUrl)
//
//        }
//        else {
//            PublicProfileCrawlerUtils.LOGGER.error("this URL does not exist: '" + stringUrl + "'")
//        }
//
//        output
//    }
//
//    def parseTwitter(document: Document, stringUrl: String): ServiceProfileDTO = {
//        val locationUrl = new URL(stringUrl)
//        val locationPath = locationUrl.getPath
//        val output = new ServiceProfileDTO
//
//        if (locationPath.matches( """/[\w\d_]+""")) {
//            val imageUrl = document.select("a[class*=profile-picture]").attr("href")
//            val name = document.select("h1[class*=fullname]").text()
//            val userId = document.select("span[class*=screen-name]").text().replaceFirst("@", "")
//            val location = document.select("span[class*=location]").text()
//            var isPublic = true
//            if (document.select("div[class*=stream-protected stream-placeholder]").size() > 0) {
//                isPublic = false
//            }
//
//            output.setServiceType(ServiceType.twitter)
//            output.setNotPublic(!isPublic)
//            output.setUserId(userId)
//            output.setFullName(name)
//            output.setImageUrl(imageUrl)
//            output.setLocation(location)
//            output.setUrl(stringUrl)
//        }
//        else {
//            PublicProfileCrawlerUtils.LOGGER.error("this URL does not exist: '" + stringUrl + "'")
//        }
//
//        output
//
//    }
//
//    def parseLinkedin(document: Document, stringUrl: String): ServiceProfileDTO = {
//        val locationUrl = new URL(stringUrl)
//        val locationPath = locationUrl.getPath
//        val linkedInServiceProfile = new ServiceProfileDTO
//        locationPath match {
//
//            case PublicProfileCrawlerUtils.LI_PROFILE_PAGE_PATTERN(matchedProfile) => {
//
//                val rawProfile = document.select("div#content")
//                val rawProfileHtml = rawProfile.html()
//
//                val profileImageElem = rawProfile.select("div#profile-picture>image.photo").first()
//                val profileImageSrc = profileImageElem match {
//                    case imageSrc: Element => {
//                        imageSrc.attr("abs:src")
//                    }
//                    case _ => ""
//
//                }
//                val profileNameElem = rawProfile.select("span.full-name").first()
//                val profileName = if (profileNameElem != null) {
//                    profileNameElem.text()
//                } else {
//                    null
//                }
//                val jobTitleElem = rawProfile.select("p.headline-title").first()
//                val jobTitle = if (jobTitleElem != null) {
//                    jobTitleElem.text()
//                } else {
//                    null
//                }
//
//                val companyElem = rawProfile.select("[class*=company-profile]").first()
//                val companyDescription = if (companyElem != null) {
//                    companyElem.text()
//                } else {
//                    ""
//                }
//
//                val geoLocation = Option(rawProfile.select("span.locality").first()).map(_.text).orNull
//                val profileDescriptionElem = rawProfile.select("div.content p.description").first()
//                val profileDescriptionContent = if (profileDescriptionElem != null) {
//                    profileDescriptionElem.html()
//                } else ""
//
//                val profileExperienceElem = rawProfile.select("div#profile-experience").first()
//                var work = new UserEmployment
//                var workLocation = ""
//                val profileExperienceContent = if (profileExperienceElem != null) {
//                    val profileCurrentWorkElem = rawProfile.select("[class*=position  first experience vevent vcard summary-current]").first()
//                    val profileCurrentWorkContent = if (profileCurrentWorkElem != null) {
//                        val title = profileCurrentWorkElem.select("[class*=title]").first().text()
//                        val company = profileCurrentWorkElem.select("[class*=org summary]").first().text()
//                        if (profileCurrentWorkElem.select("[class*=location]").first() != null) {
//                            workLocation = profileCurrentWorkElem.select("[class*=location]").first().text()
//                        }
//                        val workDescription = Option(profileCurrentWorkElem.select("[class*= description current-position]").first())
//                        val current = Option(profileCurrentWorkElem.select("[class*=dtstamp]").first())
//                        val isCurrent = current.getOrElse(profileCurrentWorkElem.select("[class*=title]").first()).text().toLowerCase.matches("present")
//                        work = UserEmployment("", company, title, workDescription.getOrElse(profileCurrentWorkElem.select("[class*=title]").first()).text(), isCurrent)
//                    }
//                    else if (rawProfile.select("[class*=position  first experience vevent vcard summary-past]").first() != null) {
//                        val profileRecentWorkElem = rawProfile.select("[class*=position  first experience vevent vcard summary-past]").first()
//                        val title = profileRecentWorkElem.select("[class*=title]").first().text()
//                        val company = profileRecentWorkElem.select("[class*=org summary]").first().text()
//                        val workDescription = Option(profileRecentWorkElem.select("[class*= description current-position]").first())
//                        val isCurrent = false
//                        work = UserEmployment("", company, title, workDescription.getOrElse(profileRecentWorkElem.select("[class*=title]").first()).text(), isCurrent)
//                    }
//                } else ""
//
//                val profileBioElement = rawProfile.select("[class*= description summary]").first()
//                val profileBioContent = if (profileBioElement != null) {
//                    profileBioElement.text()
//                } else ""
//
//
//
//                val profileEducationElem = rawProfile.select("div#profile-education").first()
//                var edu = new UserEducation
//                val profileEducationContent = if (profileEducationElem != null) {
//                    val institution = profileEducationElem.select("[class*=summary fn org]").first().text()
//                    val degree = Option(profileEducationElem.select("[class*=degree]").first())
//                    val major = Option(profileEducationElem.select("[class*=major]").first())
//                    edu = UserEducation(null, "", "", major.getOrElse(rawProfile.select("[class*=summary-education]").first()).text(), degree.getOrElse(rawProfile.select("[class*=summary-education]").first()).text(), institution, "")
//                } else ""
//
//
//                val websitesElements = rawProfile.select("li.website > a")
//                val websiteIt = websitesElements.iterator()
//                val websiteList = new StringBuilder().append("Websites: ")
//                while (websiteIt.hasNext) {
//                    val curWebsite = websiteIt.next()
//
//                    if (curWebsite != null) {
//                        val curWebsiteElem = curWebsite.asInstanceOf[Element]
//                        val websiteName = curWebsiteElem.text()
//                        curWebsiteElem.attr("href") match {
//                            case PublicProfileCrawlerUtils.LI_WEBSITE_LINK_PATTERN(curWebsiteUrl, remainingParams) =>
//                                val decoder = new java.net.URLDecoder()
//                                val extractedWebsiteUrl = java.net.URLDecoder.decode(curWebsiteUrl, "utf-8")
//                                websiteList.append(extractedWebsiteUrl).append(":")
//                            //addSocialNetwork(ServiceProfileDTO(ServiceType.Customwebsite, extractedWebsiteUrl, extractedWebsiteUrl))
//                            case _ => PublicProfileCrawlerUtils.LOGGER.error("error matching website pattern for: " + websiteName)
//                        }
//                    }
//                }
//
//                var userId = ""
//                linkedInServiceProfile.setServiceType(ServiceType.linkedin)
//                linkedInServiceProfile.setUrl(stringUrl)
//                linkedInServiceProfile.setFullName(profileName)
//                linkedInServiceProfile.setImageUrl(profileImageSrc)
//                linkedInServiceProfile.setEducation(List(edu))
//                linkedInServiceProfile.setWork(List(work))
//                linkedInServiceProfile.setLocation(workLocation)
//                linkedInServiceProfile.setBio(profileBioContent + "::" + websiteList.toString())
//                linkedInServiceProfile.setNotPublic(false)
//                locationPath match {
//                    case PublicProfileCrawlerUtils.GET_PROFILE_ID(userid) => userId = userid
//                    case _ => userId = "can't extract user ID:" + locationPath
//                }
//                linkedInServiceProfile.setUserId(userId)
//
//
//            }
//            case _ => {
//                PublicProfileCrawlerUtils.LOGGER.error("error matching URL so this is probably not a member profile page — ok to ignore: '" + stringUrl + "'")
//            }
//        }
//        linkedInServiceProfile
//    }
//
//}
//
//object PublicProfileCrawlerUtils {
//
//
//    val USER_AGENT_LIST = List("Mozilla/5.0 Galeon/1.0.2 (X11; Linux i686; U;) Gecko/20011224", "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2",
//        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1) Gecko/20061129 BonEcho/2.0", "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.1) Gecko/20061205 Iceweasel/2.0.0.1 (Debian-2.0.0.1+dfsg-2)",
//        "Mozilla/5.0 (X11; U; Linux i686; de-AT; rv:1.8.0.2) Gecko/20060309 SeaMonkey/1.0", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.1) Gecko/2008092215 Firefox/3.0.1 Orca/1.1 beta 3",
//        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:x.xx) Gecko/20030504 Mozilla Firebird/0.6", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.19 (KHTML, like Gecko) Chrome/0.2.153.1 Safari/525.19",
//        "Mozilla/5.0 (Macintosh; U; PPC Mac OS X Mach-O; en-US; rv:1.0.1) Gecko/20021219 Chimera/0.6 ", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9b5) Gecko/2008032619 Firefox/3.0b5",
//        "Mozilla/4.5 (compatible; OmniWeb/4.1-beta-1; Mac_PowerPC)", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)")
//
//    val GET_FB_USERID = """/([\w\d\.]+)""".r
//    val GET_PROFILE_ID = """/[\w]+/([\w\d%-]+)/?.*""".r
//    val GET_PROFILE_ID_FROM_FULL_LINK = """http://www.linkedin.com/[\w]{2,3}/([\w\d%-]+)/?.*""".r
//    val LI_ALIAS_PATTERN = """/in/([\w\d%-]+)""".r
//    val LI_PROFILE_PAGE_PATTERN = """(/in/[\w\d%-]+|/pub/[\w\d%-]+/[\w\d]{1,4}/[\w\d]{1,4}/[\w\d]{1,4})/?""".r
//    val FB_PROFILE_PAGE_PATTERN = """https://www.facebook.com(/.*)/?""".r
//    val LI_FULL_PROFILE_LINK_PATTERN = """.*?/profile/view\?id=([\d]+)&.*?""".r
//    val LI_TWITTER_URL_PATTERN = """.*?twitter\.com/([\w\d\-]+)(&.*|$)""".r
//    val LI_WEBSITE_LINK_PATTERN = """/redir/redirect\?url=([\w\d\%\-]+)(&.*|$)""".r
//
//
//    val LOGGER: Logger = Logger.getLogger(classOf[PublicProfileCrawlerUtils])
//
//}
//
