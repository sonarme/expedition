package com.sonar.expedition.scrawler

import com.twitter.scalding._
import scala.xml._

import factory.XMLLoader
import java.net.{URLConnection, URL}
import scala.xml._
import scala.xml.pull._
import scala.io.Source


/**
 * Created with IntelliJ IDEA.
 * User: jyotirmoysundi
 * Date: 5/24/12
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */

class MeetUpCrawl(args: Args) extends Job(args) {


    val input = TextLine("/Users/jyotirmoysundi/meetuputrls/meetup.txt")

    val output = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output6.txt")

    val output2 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output7.txt")

    val output3 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output8.txt")

    val output4 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output9.txt")

    val output5 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output12.txt")


    //read the inout file , the inout file is created after donwlaod the files manually from sitemap.xml and unziping them , finaaly a combined file is prepared out of all the unzippied files, these combined file contains all the urls in 1st and 2nd level of depth obtained from sitemap.xml

    /*var links1 = ((input
            .read
            .project('line)
            .filter('line) {
        text: String => text.contains("loc>http:")
    }
            .mapTo('line -> 'tmplinks) {
        line: String => (line.trim)
    }.mapTo('tmplinks -> 'links) {
        tmplinks: String => (tmplinks.substring(5, tmplinks.indexOf("</loc>")))
    }.flatMapTo('links -> 'profiles) {
        links: String => MeetupCrawler.extractContentsPageLinks(links).split("\\n") //will contain bth profile info and pagination links , if pages more then one, add first page as a pagination link
    }  */
     //.unique('profiles)))
   //  .write(output5)))
    //pagination links
    //var pagelinks = links1.filter(
      //  'profiles) {
        //println('profiles.toString())  ;
       // profiles: String => MeetupCrawler.checkPageLinksURL(profiles) //filter page links from 1st page to last page as seen in page 1 of a group, at max 10 is seen., need to make it more then 10 and has to be reconfigurable.

    //}
    //  .write(output5) ))


    //start of sitemap
    var links1=((input.read
            .project('line)
            .flatMap('line->'links){
                line:String => MeetupCrawler.importLinks(line).split("\\n")
            }.flatMapTo('links -> 'profiles) {
                links: String => MeetupCrawler.extractContentsPageLinks(links).split("\\n") //will contain  pagination links , if pages more then one, add first page as a pagination link
            } ))
            //.write(output5)))
    //end of sitemap
    //profiles from paginations links
    var proffromURL = links1.flatMap('profiles -> 'profiles) {
        profiles: String => MeetupCrawler.extractContentsfromPageLinks(profiles).split("\\n") //get profile page links from each page
        //profiles: String => profiles
    }.filter('profiles) {
        innerprofiles: String => MeetupCrawler.checkURL(innerprofiles) //filter profile pages
    }.unique('profiles)
            .mapTo('profiles -> 'meetupinfo) {
        profiles: String => MeetupCrawler.getProfileInfo(profiles) //get actual contents and write to a tap sink

    }
            .write(output3)


}