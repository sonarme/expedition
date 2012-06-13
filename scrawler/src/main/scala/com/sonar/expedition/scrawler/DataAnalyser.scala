import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import DataAnalyser._
import util.matching.Regex

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
    var out = "/tmp/results5.txt"
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
            var value4 = getValue4(serviceType, serviceType2, jsondata2)
            var value5 = getValue5(serviceType, serviceType2, jsondata2)
            (fields._1, fields._2, fields._3, value4, value5)
    }.project(Fields.ALL)
            .mapTo(Fields.ALL -> Fields.ALL) {
        fields: (String, String, String, String, String) =>
            val (id, serviceType, jsondata, serviceType2, jsondata2) = fields

            //var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(jsondata.toString.getBytes())); // can reuse, share globally
            var fbid = getFBID(jsondata)
            //var lndata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(jsondata2.toString.getBytes())); // can reuse, share globally
            var lnid = getFBID(jsondata2) //"2"//lndata.getImageUrl()
            (id, fbid, jsondata, lnid, jsondata2)
    }.project(Fields.ALL)
            .write(TextLine(out))

    def getFBID(input: String): String = {
        if (input == null) {
            "null"
        } else {
            try {


                var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(input.getBytes()));
                if (fbdata == null) {
                    "null"
                } else {
                    fbdata.getUserId()
                }
            } catch {
                case e: Exception => "null"
            }

        }

    }

    def getLNID(input: String): String = {
        if (input == null) {
            "null"
        } else {
            var lndata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(input.getBytes()));
            if (lndata == null) {
                "null"
            } else {
                lndata.getUserId()
            }
        }

    }

    def getValue4(inp1: String, inp2: String, inp3: String): String = {

        if (inp1 == inp2) {
            "null"
        }
        else {
            inp2
        }

    }

    def getValue5(inp1: String, inp2: String, inp3: String): String = {

        if (inp1 == inp2) {
            "null"
        }
        else {
            inp3
        }

    }

}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
}



