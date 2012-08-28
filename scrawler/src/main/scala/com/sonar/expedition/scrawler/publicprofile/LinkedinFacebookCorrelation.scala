//package com.sonar.expedition.scrawler.publicprofile
//
//import com.twitter.scalding.{SequenceFile, TextLine, Args}
//import com.sonar.dossier.dto.ServiceProfileDTO
//import org.jsoup.nodes.Document
//import cascading.scheme.hadoop.WritableSequenceFile
//
//class LinkedinFacebookCorrelation(args: Args) extends Job(args) {
//    var crawler = new PublicProfileCrawlerUtils
////    val output = new WritableSequenceFile("/tmp/idkwhatimdoing.seq")
//    var profileLinks = (TextLine("/Users/user1/smallLinkedin.txt").read.project('line))
//            .mapTo('line ->('userid, 'alternateUserid, 'lndoc)) {
//        fields: String =>
//            val line = fields
//            val lndoc = crawler.getPageContentsDoc(line)
//            Thread.sleep(500)
//            val linkedin = crawler.parseLinkedin(lndoc, line)
//            var userId = ""
//            line match {
//                case PublicProfileCrawlerUtils.GET_PROFILE_ID_FROM_FULL_LINK(userid) => userId = userid
//                case _ => userId = "can't extract user ID:" + line
//            }
//            var alternateUserid = ""
//            if (userId.contains("-")) {
//                val fname = userId.split("-").head
//                val lname = userId.split("-").last
//                alternateUserid = fname.charAt(0) + lname
//            }
//            userId = userId.replaceAll("-", "")
//            (userId, alternateUserid, linkedin)
//    }.map(('userid, 'alternateUserid, 'lndoc) ->('fbdoc, 'twdoc, 'score)) {
//        fields: (String, String, ServiceProfileDTO) =>
//            val (userid, alternateUserid, linkedin) = fields
//            val fblink = "https://www.facebook.com/" + userid
//            val alternatefbLink = "https://www.facebook.com/" + alternateUserid
//            val twlink = "http://twitter.com/" + userid
//            val alternatetwLink = "http://twitter.com/" + alternateUserid
//            val fbdoc = crawler.getFBPageContentsDoc(fblink)
//            Thread.sleep(500)
//            val alternatefbDoc = crawler.getFBPageContentsDoc(alternatefbLink)
//            Thread.sleep(500)
//            val twdoc = crawler.getPageContentsDoc(twlink)
//            Thread.sleep(500)
//            val alternatetwDoc = crawler.getPageContentsDoc(alternatetwLink)
//            Thread.sleep(500)
//            var facebook = new ServiceProfileDTO
//            var twitter = new ServiceProfileDTO
//            if (fbdoc.body().hasText == true) {
//                facebook = crawler.parseFacebook(fbdoc, fblink)
//            } else if (alternatefbDoc.body().hasText == true) {
//                facebook = crawler.parseFacebook(alternatefbDoc, alternatefbLink)
//            } else {
//                facebook = null
//            }
//
//            if (twdoc.select("div[class*=body-content]").select("h1").text().matches("Sorry, that page doesn’t exist!") != true) {
//                twitter = crawler.parseTwitter(twdoc, twlink)
//            } else if ((!alternateUserid.isEmpty) && (alternatetwDoc.select("div[class*=body-content]").select("h1").text().matches("Sorry, that page doesn’t exist!") != true)) {
//                twitter = crawler.parseTwitter(alternatetwDoc, alternatetwLink)
//            } else {
//                twitter = null
//            }
//
//            var correlationScore = 0
//
//            if (linkedin.getFullName() != null) {
//                if (facebook != null) {
//                    if (linkedin.getFullName().equalsIgnoreCase(facebook.getFullName())) {
//                        correlationScore += 1
//                    }
//                }
//                if (twitter != null) {
//                    if (linkedin.getFullName().equalsIgnoreCase(twitter.getFullName())) {
//                        correlationScore += 1
//                    }
//                }
//            }
//
//
//            (facebook, twitter, correlationScore)
//    }/*.project('userid, 'alternateUserid, 'score, 'fbdoc, 'twdoc, 'lndoc)*/.project('userid, 'score).write(TextLine("/tmp/blahblah.txt"))
////        .write(WritableSequenceFile(('userid,'score), (String, Int))
//
//
//
//}
