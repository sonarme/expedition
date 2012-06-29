import cascading.pipe.Pipe
import org.jsoup.nodes.Document
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpPost
import cascading.tuple.Fields
import com.restfb.types.User.Education
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO}
import com.sonar.expedition.scrawler.{FriendObjects, CheckinObjects}
import com.twitter.scalding._
import java.nio.ByteBuffer
import java.security.MessageDigest
import scala.{Some, Option}
import util.matching.Regex

import scala.collection.JavaConversions._
import com.sonar.expedition.scrawler.uniqueCompanies
import com.twitter.scalding._
import cascading.tuple.Fields
import com.restfb.types.User.Education
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO}
import com.sonar.expedition.scrawler.{FriendObjects, CheckinObjects}
import com.twitter.scalding._
import java.nio.ByteBuffer
import java.security.MessageDigest
import scala.{Some, Option}
import util.matching.Regex

import scala.collection.JavaConversions._
import com.sonar.expedition.scrawler.uniqueCompanies
import com.sonar.expedition.scrawler.MeetupCrawler
import com.sonar.expedition.scrawler._


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
//public class RestCallApi(args: Args) extends Job(args){
class RestCallApi(args: Args){
    var inputfile="/tmp/companies.txt"
    var outfile="/tmp/companies_locate.txt"
    var links1 = TextLine(inputfile).read.project('line).mapTo('line ->('corp,'location)){
        line:String =>
        //val json_results =  fourSquareCall(line,line)
        //(line,json_results)
            ("test","test")

    }.project('corp,'location).write(TextLine(outfile))


    def fourSquareCall(query:String,location:String) : String = {

        val fsquery = "sonar"
        val fslocation="40.714353,-74.005973";
        val url= "https://api.foursquare.com/v2/venues/search?ll="+fslocation+"&query="+fsquery+"&client_id=NB45JIY4HBP3VY232KO12XGDAZGF4O3DKUOBRTGZ5REY50E1&client_secret=5NCZW0FWUCHCJ5VS35YDG20AYHGBC2H5Z1W2OIG13IUEDHNK";
        var responseBodyString = HttpClientRestApi.fetchresponse(url)
        responseBodyString


    }

}

Object RestCallApi{
    def fourSquareCall(query:String,location:String) : String = {

        val fsquery = "sonar"
        val fslocation="40.714353,-74.005973";
        val url= "https://api.foursquare.com/v2/venues/search?ll="+fslocation+"&query="+fsquery+"&client_id=NB45JIY4HBP3VY232KO12XGDAZGF4O3DKUOBRTGZ5REY50E1&client_secret=5NCZW0FWUCHCJ5VS35YDG20AYHGBC2H5Z1W2OIG13IUEDHNK";
        var responseBodyString = HttpClientRestApi.fetchresponse(url)
        responseBodyString


    }
}


