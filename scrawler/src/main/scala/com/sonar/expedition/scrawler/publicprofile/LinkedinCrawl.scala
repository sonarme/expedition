//package com.sonar.expedition.scrawler.publicprofile
//
//import com.twitter.scalding.{RichPipe, Args}
//import com.sonar.expedition.scrawler.publicprofile.PublicProfileCrawlerUtils
//import org.jsoup.nodes.{Element, Document}
//import java.net.URL
//import com.sonar.dossier.dto.ServiceProfileDTO
//
//
//class LinkedinCrawl(args: Args) extends Job(args) {
//    //    LinkedinCrawler.importLinks("/tmp/linkedinSitemapData2teeeeest.txt")
//    var crawler = new PublicProfileCrawlerUtils
//    var profileLinks = (TextLine("/tmp/linkedinSitemapData2teeeeest.txt").read.project('line))
//            .filter('line) {
//        line: String => crawler.checkIfLIProfileURL(line)
//    }.map('line ->('docs, 'line)) {
//        fields: (String) =>
//            val line = fields
//            val docs = crawler.getPageContentsDoc(line)
//            (docs, line)
//    }.map(('docs, 'line) -> 'profiles) {
//        fields: (Document, String) =>
//            val (docs, stringUrl) = fields
//            val parsed = crawler.parseLinkedin(docs, stringUrl)
//            parsed
//    }.project('profiles).write(TextLine("/tmp/linkedinProfileCrawlResults.txt"))
//
//
//}
//
