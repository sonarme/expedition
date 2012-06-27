
import com.twitter.scalding.{Job, Args}

import com.restfb.types.User.Education
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.dossier.dto.UserEducation
import com.sonar.dossier.dto.UserEmployment
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO}
import com.sonar.expedition.scrawler._
import com.twitter.scalding._
import com.twitter.scalding.TextLine
import java.nio.ByteBuffer
import java.security.MessageDigest
import scala.Some
import scala.{Some, Option}
import util.matching.Regex
import FactualQueryByZip._

import scala.collection.JavaConversions._
import util.parsing.json.{JSONArray, JSONObject}

class FactualQueryByZip(args: Args) extends Job(args) {

    val inputData = "/tmp/zipssmall.txt"
    val wrtData = TextLine("/tmp/zipsout.txt")


    val data = (TextLine(inputData).read.project('line).mapTo(('line) ->('zipcode)) {
        line: String => {
            line match {
                case zipcode(id,zip,state,name,lat,lon,code,dist) => (zip)
                case _ => ("None")
            }
        }
    }).project('zipcode).flatMap('zipcode->'name){
        line:String => getFactualRes(line).split("\\n")
    }.write(wrtData)

    def getFactualRes(zip:String):String = {
        val hp = new HttpClientRest()
        hp.getFactualLocsFrmZip(zip);

    }

}

object FactualQueryByZip{
   val zipcode="""(.*),"(.*)",(.*),(.*),(.*),(.*),(.*),(.*)""".r
}
