package com.sonar.expedition.scrawler.linkedin

import com.twitter.scalding._
import com.sonar.expedition.scrawler.linkedin.LinkedinCrawlerUtils
import org.jsoup.nodes.{Element, Document}
import java.net.URL

/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

 */
class LinkedinCrawl(args: Args) extends Job(args) {
    //val input = TextLine("meetuptest.txt")
    //read the inout file , the inout file is created after donwlaod the files manually from sitemap.xml and unziping them , finaaly a combined file is prepared out of all the unzippied files, these combined file contains all the urls in 1st and 2nd level of depth obtained from sitemap.xml

    /*var links1 =
        (
                (
                        //TextLine(args.apply("input"))
                        input     .read
                                .project('line)
                                .filter('line) {
                            text: String => text.contains("loc>http:")
                        }.mapTo('line -> 'tmplinks) {
                            line: String => (line.trim)
                        }.mapTo('tmplinks -> 'links) {
                            tmplinks: String => (tmplinks.substring(5, tmplinks.indexOf("</loc>")))
                        }
                                .flatMapTo('links -> 'memberlinks) {
                            links: String => ScrawlerUtils.extractContentsPageLinks(links).split("\\n") //will contain bth profile group info and pagination links , if pages more then one, add first page as a pagination link
                        }//.write(output5)
                        )
                ) */

    //start of sitemap
    LinkedinCrawler.importLinks("/tmp/linkedinSitemapData2teeeeest.txt")
    var crawler = new LinkedinCrawlerUtils
    var profileLinks = (TextLine("/tmp/linkedinSitemapData2teeeeest.txt").read.project('line))
            .filter('line) {
        line: String => crawler.checkIfProfileURL(line)
    }
            .map('line -> ('docs, 'line)) {
        fields: (String) =>
            val line = fields
            val docs = crawler.getPageContentsDoc(line)
//            docs = LinkedinCrawlerUtils.getPageContentsDoc(line)
            (docs, line)
    }.map(('docs, 'line) -> 'profiles) {
        fields: (Document, String) =>
            val (docs, stringUrl) = fields
            val parsed = crawler.parseJsoup(docs, stringUrl)
            parsed
    }
            //            .flatMapTo('links -> 'profiles) {
            //        links: String => ScrawlerUtils.extractContentsPageLinks(links).split("\\n") //will contain  pagination links , if pages more then one, add first page as a pagination link
            //    }
            .write(TextLine("/tmp/finaloutput.txt"))
    //end of sitemap

    //profiles from paginations links
    //    var proffromURL = links1
    //            .flatMap('memberlinks -> 'profiles) {
    //        profiles: String => ScrawlerUtils.extractContentsfromPageLinks(profiles).split("\\n") //get profile page links from each page
    //    }.filter('profiles) {
    //
    //        profiles: String => ScrawlerUtils.checkIfProfileURL(profiles) //filter profile pages   , checkURL in java file
    //    }.unique('profiles)
    //            .mapTo('profiles -> 'infofrommeetup) {
    //        profiles: String => ScrawlerUtils.getProfileInfo(profiles) //get actual contents and write to a tap sink
    //    }.write(TextLine(args.apply("output")))


}

