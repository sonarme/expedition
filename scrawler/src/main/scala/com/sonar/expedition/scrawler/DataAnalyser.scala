import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.{UserEducation, ServiceProfileDTO}
import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import DataAnalyser._
import util.matching.Regex

import com.sonar.dossier.dto.UserEducation

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
class DataAnalyser(args: Args) extends Job(args) {

    var inputData = "/tmp/dataAnalyse.txt"
    var out = "/tmp/results7.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => (userProfileId, serviceType, json)
                case _ => ("", "", "")
            }
        }
    }).project('id, 'serviceType, 'jsondata)

    //.write(TextLine(out)
    //Now tuple contains 'id, 'serviceType, 'jsondata,'numProfiles
    val numProfiles = data.groupBy('id) {
        _.size
    }.rename('size -> 'numProfiles)

    var out2 = TextLine("/tmp/data123.txt")
    var rest = data.groupBy('id) {
        fields: (String, String, String) =>
            val (id: String, serviceType: String, jsondata: String) = fields
            (id, _.mkString('serviceType, ","), _.mkString('jsondata, ","))

    }.write(out2)
    // Merge `serviceType` with `numProfiles`, by joining on their id fields.
    val profiles =
        data.joinWithSmaller('id -> 'id, numProfiles)

    //join profiles againts itself to get all corelated ids(fb and linked correlation i am talking about :) )


    val dupProfiles =
        profiles.rename(('id, 'serviceType, 'jsondata, 'numProfiles) ->('id2, 'serviceType2, 'jsondata2, 'numProfiles2))


    val joinedProfiles = profiles.joinWithSmaller('id -> 'id2, dupProfiles).project('id, 'serviceType, 'jsondata, 'serviceType2, 'jsondata2)

            /*val resultant1 = joinedProfiles.filter('serviceType, 'serviceType2) {
                serviceType : (String, String) => (serviceType._1 != serviceType._2) || serviceType._1==null || serviceType._2==null
            }

            var resultant2 = joinedProfiles.filter('serviceType, 'serviceType2) {
                serviceType : (String, String) => (serviceType._1 == serviceType._2)
            }.project('id,'serviceType,'jsondata)
            */
            .mapTo(Fields.ALL -> Fields.ALL) {
        fields: (String, String, String, String, String) =>
            val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
            var value2 = getIDType2(serviceType, serviceType2)
            var value3 = getJson3(serviceType, serviceType2, jsondata)
            var value4 = getIDType4(serviceType, serviceType2)
            var value5 = getJson5(serviceType, serviceType2, jsondata2)
            // if (value4==null && value5 ==null){
            //   (fields._1, value4, value5, value4, value5)
            //}else{
            (fields._1, value2, value3, value4, value5)
        //}

    }.project(Fields.ALL)
            .mapTo(Fields.ALL -> Fields.ALL) {
        fields: (String, String, String, String, String) =>
            val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
            var fbid = getID(jsondata)
            var lnid = getID(jsondata2) //"2"//lndata.getImageUrl()
            (id, fbid, jsondata, lnid, jsondata2)
    }
            .project(Fields.ALL)
            //.mapTo(Fields.ALL -> 'fb_id, 'lnid, 'fname, 'lname, 'currentcity, 'worklocation, 'employer, 'jobtype, 'workhistory, 'education, 'likes,'friends)
            .mapTo(Fields.ALL ->('rowkey, 'fbid, 'lnid, 'work)) {
        fields: (String, String, String, String, String) =>
            val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
            var fbid = getID(jsondata)
            var lnid = getID(jsondata2)
            //var fname       = getFbFirstName(jsondata)
            //var lname       = getFbLastName(jsondata)
            //var currentcity = getCurrentCity(jsondata)
            //var worklocation= getWorkLocation(jsondata)
            (fields._1, fbid, lnid, getFBInfo(jsondata2))
    }.project(Fields.ALL)
            .write(TextLine(out))

    def getFBInfo(input: String): List[UserEducation] = {
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(input.toString.getBytes()));
        //println(fbdata.getWork()groupBy)
        fbdata.getEducation().toList
    }

    def getIDType2(inp1: String, inp2: String): String = {

        if ((inp1 == inp2) && inp1.trim == "ln") {
            "null"
        }
        else {
            inp2
        }
    }

    def getIDType4(inp1: String, inp2: String): String = {

        if ((inp1 == inp2) && inp1.trim == "fb") {
            "null"
        }
        else {
            inp2
        }
    }

    def getJson3(inp1: String, inp2: String, inp3: String): String = {

        if (inp1 == inp2 && inp1.trim == "ln") {
            "null"
        }
        else {
            inp3
        }
    }

    def getJson5(inp1: String, inp2: String, inp3: String): String = {

        if (inp1 == inp2 && inp1.trim == "fb") {
            "null"
        }
        else {
            inp3
        }
    }

    def getID(input: String): String = {
        if (input == null) {
            "null"
        } else {
            try {
                var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(input.toString.getBytes()));
                if (fbdata == null) {
                    "null"
                } else {
                    fbdata.getUserId()
                }
            } catch {
                case e: Exception => null
            }

        }

    }


}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
}



