package com.sonar.expedition.scrawler

import com.twitter.scalding._
import scala.xml._

import factory.XMLLoader
import java.net.{URLConnection, URL}
import scala.xml._
import scala.xml.pull._
import scala.io.Source


import com.twitter.scalding._
import scala.xml._

import factory.XMLLoader
import java.net.{URLConnection, URL}
import scala.xml._
import scala.xml.pull._
import scala.io.Source
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import org.kohsuke.args4j.CmdLineParser
import org.springframework.util.FileCopyUtils
import java.io.InputStreamReader
import java.net.MalformedURLException
import java.net.URL
import java.net.URLConnection
import java.util.Collection
import java.util.Date
import java.util.regex.Pattern
import java.io._
import java.net._
import org.jsoup._
import org.jsoup.nodes.Document
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.jsoup.Jsoup
import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params.HttpMethodParams
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import java.net.HttpURLConnection;
import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: jyotirmoysundi
 * Date: 5/24/12
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */

/*


run the code with two arguments passed to it.
input1 : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to
input1 /Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output8.txt
input2 /Users/jyotirmoysundi/meetuputrls/meetuptest.txt
output /Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output9.txt

 */
class MeetUpCrawl(args: Args) extends Job(args) {


    /*val input = TextLine("/Users/jyotirmoysundi/meetuputrls/meetuptest.txt")

    val output = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output6.txt")

    val output2 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output7.txt")

    val output3 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output8.txt")

    val output4 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output9.txt")

    val //output5 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output12.txt")

    */
    /*
   test =code =to =be =tested =later
   //read the inout file , the inout file is created after donwlaod the files manually from sitemap.xml and unziping them , finaaly a combined file is prepared out of all the unzippied files, these combined file contains all the urls in 1st and 2nd level of depth obtained from sitemap.xml

  //var prevreads = TextLine(args.apply("input1")).read.project('line).flatMap('line->'profiles){line:String => line.split(",")
  /*.filter('profiles){
      profiles:String =>
          val Pattern = "([0-9])".r
          profiles.charAt(0) match {
              case Pattern(c) => true
              case _ =>   false
          }
   */
  //}.write(output2)
  //.groupBy('profiles).write(output2)
  //project('line).write(output2)

  /* var links1 =
    (
        (
         TextLine(args.apply("input2"))
                 .read
        .project('line)
        .filter('line) {
            text: String => text.contains("loc>http:")
        }.mapTo('line -> 'tmplinks) {
            line: String => (line.trim)
        }.mapTo('tmplinks -> 'links) {
            tmplinks: String => (tmplinks.substring(5, tmplinks.indexOf("</loc>")))
        }
        .flatMapTo('links -> 'profiles) {
            links: String => extractContentsPageLinks(links).split("\\n") //will contain bth profile group info and pagination links , if pages more then one, add first page as a pagination link
        }//.write(output5)
        )
     )
  */
  //.unique('profiles)))
  // .write(output5)))
  //pagination links
  //var pagelinks = links1.filter(
  //  'profiles) {
  //println('profiles.toString())  ;
  // profiles: String => MeetupCrawler.checkPageLinksURL(profiles) //filter page links from 1st page to last page as seen in page 1 of a group, at max 10 is seen., need to make it more then 10 and has to be reconfigurable.

  //}

    */
    val output4 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output13.txt")
    val output2 = TextLine("/Users/jyotirmoysundi/Developer/Workspace/dossier/scalfold/src/main/resources/tutorial/data/output7.txt")


    //start of sitemap

    var links1 = ((TextLine(args.apply("input2")).read
            .project('line)
            .flatMap('line -> 'links) {
        line: String => MeetupCrawler.importLinks(line).split("\\n")
    }.flatMapTo('links -> 'profiles) {
        links: String => extractContentsPageLinks(links).split("\\n") //will contain  pagination links , if pages more then one, add first page as a pagination link
    }))

    //.write(output5)))
    //end of sitemap

    //profiles from paginations links

    var proffromURL = links1.flatMap('profiles -> 'profiles) {
        profiles: String => extractContentsfromPageLinks(profiles).split("\\n") //get profile page links from each page
        //profiles: String => profiles
    }.filter('profiles) {
        innerprofiles: String => checkIfProfileURL(innerprofiles) //filter profile pages   , checkURL in java file
    }.unique('profiles)
            .mapTo('profiles -> 'meetupinfo) {
        profiles: String => getProfileInfo(profiles) //get actual contents and write to a tap sink
    }
            .write(TextLine(args.apply("output")))


    def extractContentsfromPageLinks(urlpass: String): String = {
        var document: Document = null
        var results: String = ""
        try {
            //System.out.println("sundi url " + urlpass)
            Thread.sleep(1000)
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get
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
        catch {
            case e: Exception => {
                e.printStackTrace
                "error fetching"
            }
        }
    }

    def checkIfProfileURL(url: String): Boolean = {
        val pattern: Pattern = Pattern.compile(".*\\d{7,8}/");

        val matcher: Matcher = pattern.matcher(url);

        matcher.matches();
    }

    def extractContentsPageLinks(url: String): String = {
        var results = ""
        try {
            val highestpage = findhighestepageNum(url);
            System.out.println("urls4 " + highestpage)
            val index = url.indexOf("meetup.com") + 11;
            var groupname: String = url.substring(index, index + url.substring(index).indexOf("/"));
            if (1 == highestpage) {
                val tmpurl = "http://www.meetup.com/" + groupname + "/members/";
                results += tmpurl + "\n";
                //System.out.println("urls1" +results)
            } else {
                //System.out.println("urls2" +results)
                for (i <- 0 until highestpage + 1) {
                    //System.out.println("urls3" +results)
                    val tmpurl = "http://www.meetup.com/" + groupname + "/members/?offset=" + i * 20 + "&desc=1&sort=chapter_member.atime";
                    results += tmpurl + "\n";
                }
            }
            //for(int i=0;i<=highestpage;i++){
            //val tmpurl="http://www.meetup.com/" + groupname +"/members/?offset="+i*20 +"&desc=1&sort=chapter_member.atime";
            //results+=tmpurl + "\n";
            //}
            System.out.println("urls" + results)
        } catch {
            case e: Exception => results + "\n" + "error fetching".toString;
        }
        results

    }

    def findhighestepageNum(url: String): Int = {

        //  10

        val client = new DefaultHttpClient
        val post = new HttpPost(url)
        val response = client.execute(post)
        //response

        var highestPage = 1;
        var tmppage = 1;
        val contents = "";
        try {


            var respcontents = ""
            var memurl = ""
            try {


                respcontents = MeetupCrawler.doHttpUrlConnectionAction(url);
                memurl = respcontents.split("relative_page")(2);
                //System.out.println("highest page number memurl: " + memurl);

                if (memurl.indexOf("?offset=") != -(1)) {

                    val tmpindex = memurl.indexOf("?offset=") + 8;
                    //tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +memurl.substring(tmpindex).indexOf("&amp;")));
                    //tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +memurl.substring(tmpindex).indexOf("&amp;")-1));
                    val innertmpind = memurl.substring(tmpindex).indexOf("&");
                    //System.out.println(memurl.substring(tmpindex) + "," + innertmpind +"," + tmpindex);
                    tmppage = Integer.parseInt(memurl.substring(tmpindex, tmpindex + innertmpind));

                    if (tmppage > highestPage)
                        highestPage = tmppage;

                }


                //highestPage
                val page: Int = highestPage match {
                    case 1 => 1
                    case _ => highestPage / 20
                }
                page

            } catch {
                case e: Exception => 1
            }


        }
        catch {

            case e: Exception => 1 //To change body of catch statement use File | Settings | File Templates.

        }


    }


    def doHttpUrlConnectionAction(desiredUrl: String): String = {

        try {

            val u = new URL(desiredUrl)
            val con: URLConnection = u.openConnection();
            val conn: HttpURLConnection = con.asInstanceOf[HttpURLConnection]
            conn.setRequestMethod("POST");

            // uncomment this if you want to write output to this url
            conn.setDoOutput(true);

            // give it 15 seconds to respond
            conn.setReadTimeout(15 * 1000);

            conn.setRequestProperty("USER-AGENT", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");

            conn.setRequestProperty("Content-type", "application/x-www-form-urlenCcoded");
            conn.setAllowUserInteraction(true);

            //loadCookies(conn)

            conn.setDoOutput(true)
            conn.connect

            var reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            var stringBuilder = new StringBuilder("");

            var line = "";
            try {
                do {
                    line = reader.readLine()
                } while (line != null)
            } catch {
                case e: Exception =>
            }
            stringBuilder.append(line);

            stringBuilder.toString();
        }
        catch {
            case e: Exception => "error"
        }


    }

    def getResponse(reader: BufferedReader): Option[String] = {
        // Wrap the Java result in an Option (this will become a Some or a None)
        Option(reader.readLine().toString);
    }

    def getProfileInfo(urlpass: String): String = {

        Thread.sleep(1000)
        // meetupid , fbid, twitter id, linkedin id, groupname_in meetup,location tuple in CSV
        val res: String = extractProfileMeetUpId(urlpass) + "," + extractProfileFBId(urlpass) + "," + extractProfileTWId(urlpass) + "," + extractProfileLINKEDId(urlpass) + "," + extractProfileGrpName(urlpass) + "," + extractProfileLocation(urlpass)
        res
    }


    def extractProfileMeetUpId(url: String): String = {

        try {
            //System.out.println()
            url.substring(url.indexOf("members/") + 8, url.length() - 1);
        } catch {
            case e: Exception => {
                e.printStackTrace
                "error fetching"
            }
        }

    }

    def extractProfileGrpName(url: String): String = {

        try {
            url.substring(url.indexOf("com/") + 4, url.indexOf("/members/"));
        } catch {
            case e: Exception => {
                e.printStackTrace
                "error fetching"
            }
        }

    }


    def extractProfileFBId(urlpass: String): String = {

        var document: Document = null;
        var results: String = "";
        try {
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            var elem: Elements = document.getElementsByClass("badge-facebook-24");
            var urlID: String = elem.attr("abs:href");
            //return urlID.substring(urlID.indexOf(".com/")+5,urlID.length());
            if (urlID.indexOf("id=") != -1) {
                urlID.substring(urlID.indexOf("id=") + 3, urlID.length());
            } else {
                urlID.substring(urlID.indexOf("www.facebook.com") + 17);
            }

        } catch {
            case e: Exception => "0";
        }


    }

    def extractProfileTWId(urlpass: String): String = {

        var document: Document = null;
        var results: String = "";
        try {
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            var elem: Elements = document.getElementsByClass("badge-twitter-24");
            var urlID: String = elem.attr("abs:href");
            //return urlID.substring(urlID.indexOf(".com/")+5,urlID.length()-1);
            urlID.substring(urlID.indexOf("twitter.com") + 12, urlID.length() - 1);
        } catch {
            case e: Exception => "0";
        }

    }


    def extractProfileLINKEDId(urlpass: String): String = {

        var document: Document = null;
        var results: String = "";
        try {
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            var elem: Elements = document.getElementsByClass("badge-linkedin-24");
            var urlID: String = elem.attr("abs:href");
            //return urlID.substring(urlID.indexOf(".com/")+5,urlID.length()-1);
            urlID.substring(urlID.indexOf("linkedin.com") + 16, urlID.length());
        } catch {
            case e: Exception => "0";
        }


    }

    def extractProfileLocation(urlpass: String): String = {

        var document: Document = null;
        var results: String = ""
        var locality: String = ""
        var region: String = ""
        var country: String = ""
        try {
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            var elem: Elements = document.getElementsByClass("locality");
            locality = elem.text();

            try {
                elem = document.getElementsByClass("region");
                region = elem.text().split(" ")(0);
                try {
                    //int address=document.text().indexOf("addressCountry")+16;
                    //String endindex=document.text().substring(address);
                    ///country = document.text().substring(address, address+ endindex.indexOf("</")) ;
                    //  country =     document.text();
                } catch {
                    case e: Exception => "0";
                }



                locality + " " + region + " " + country;

            } catch {
                case e: Exception => "0";

            }
        }
    }
}



