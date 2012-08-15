//package com.sonar.expedition.scrawler.publicprofile
//
//import com.twitter.scalding.{TextLine, Job, Args}
//import org.jsoup.nodes.Document
//
//class FacebookPublicProfileParse(args: Args) extends Job(args) {
//    var crawler = new PublicProfileCrawlerUtils
//    var profileLinks = (TextLine("/tmp/facebookUserHandles.txt").read.project('line))
//            .mapTo('line -> 'link) {
//       fields: String =>
//            val line = fields
//            val link = "https://www.facebook.com/" + line
//            link
//    }.map('link -> 'doc) {
//        fields: (String) =>
//            val link = fields
//            val doc = crawler.getFBPageContentsDoc(link)
//            (doc)
//    }.project('doc, 'link).map(('doc, 'link) -> 'profile) {
//        fields: (Document, String) =>
//            val (doc, stringUrl) = fields
//            val parsed = crawler.parseFacebook(doc, stringUrl)
//            parsed
//    }.project('profile).write(TextLine("/tmp/facebookPublicProfiles.txt"))
//
//
//}
