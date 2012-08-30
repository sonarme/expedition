package com.sonar.expedition.scrawler.test

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import java.security.MessageDigest
import cascading.tuple.Fields
import com.twitter.scalding.TextLine


/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

*/


// JUST FOR TESTING
class InternalAnalysis(args: Args) extends Job(args) with DTOProfileInfoPipe {

    val inputData = args("serviceProfileData")
    val profileCount = args("profileCount")
    val serviceCount = args("serviceCount")
    val geoCount = args("geoCount")

    val data = TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }.project(('id, 'serviceType, 'jsondata))

    val stcount = data.groupBy('serviceType) {
        _.size
    }.project(Fields.ALL).write(TextLine(serviceCount))

    val pfcount = data.unique('id).groupAll {
        _.size
    }
            .write(TextLine(profileCount))


    val gcount = getDTOProfileInfoInTuples(data)
            .map('city -> 'cityCleaned) {
        city: String => {
            StemAndMetaphoneEmployer.removeStopWords(city)
        }
    }
            .project(('key, 'cityCleaned))
            .groupBy('cityCleaned) {
        _.size
    }
            .filter('size) {
        size: Int => {
            size > 1
        }
    }.write(TextLine(geoCount))
    //'rowkey, 'username, 'fbid, 'lnid, 'fbedu, 'lnedu, 'fbwork, 'lnwork, 'city

}
