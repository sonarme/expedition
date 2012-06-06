import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._

/**
 * Created with IntelliJ IDEA.
 * User: jyotirmoysundi
 * Date: 5/24/12
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */

/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

 */
class MeetUpCrawl(args: Args) extends Job(args) {
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
    var links1 = ((TextLine(args.apply("input")).read
            .project('line)
            .flatMap('line -> 'links) {
        line: String => MeetupCrawler.importLinks(line).split("\\n")
    }.flatMapTo('links -> 'profiles) {
        links: String => ScrawlerUtils.extractContentsPageLinks(links).split("\\n") //will contain  pagination links , if pages more then one, add first page as a pagination link
    }))
    //end of sitemap

    //profiles from paginations links
    var proffromURL = links1
            .flatMap('memberlinks -> 'profiles) {
        profiles: String => ScrawlerUtils.extractContentsfromPageLinks(profiles).split("\\n") //get profile page links from each page
    }.filter('profiles) {
        innerprofiles: String => ScrawlerUtils.checkIfProfileURL(innerprofiles) //filter profile pages   , checkURL in java file
    }.unique('profiles)
            .mapTo('profiles -> 'infofrommeetup) {
        profiles: String => ScrawlerUtils.getProfileInfo(profiles) //get actual contents and write to a tap sink
    }.write(TextLine(args.apply("output")))


}



