package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import DataAnalyser._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import com.twitter.scalding.TextLine
import java.security.MessageDigest
import cascading.tuple.Fields


/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

*/


class InternalAnalysis(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val profilecount = args("profileCount")
    val servicetypecount = args("serviceCount")
    val geocount = args("geoCount")

    val employerGroupedServiceProfilePipe = new DTOProfileInfoPipe(args)

    val data = TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }.project(('id, 'serviceType, 'jsondata))

    val stcount = data.groupBy('serviceType) {
        _.size
    }.project(Fields.ALL).write(TextLine(servicetypecount))

    val pfcount = data.unique('id).groupAll {
        _.size
    }
            .write(TextLine(profilecount))


    val gcount = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data)
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
    }
            .write(TextLine(geocount))
    //'rowkey, 'username, 'fbid, 'lnid, 'fbedu, 'lnedu, 'fbwork, 'lnwork, 'city

}