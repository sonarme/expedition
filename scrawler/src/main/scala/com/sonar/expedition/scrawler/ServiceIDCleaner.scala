package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.CheckinObjects
import java.security.MessageDigest

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import ServiceIDCleaner._
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}

class ServiceIDCleaner(args: Args) extends Job(args) {


    // extracts key data for employerfinder to use. no longer groups
    var inputData = "/tmp/serviceIDs.txt"
    var out = "/tmp/serviceIDsCleaned.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('userID, 'oneID, 'twoID)) {
        line: String => {
            line match {
                case DataExtractLine(userID, oneID, twoID) => (userID, oneID, twoID)
                case _ => ("None","None","None")
            }
        }
    })
            .groupBy('userID) {
        group => group
                .toList[String]('oneID,'first).sortBy('oneID)
                .toList[String]('twoID, 'second).sortBy('twoID)
    }
            .project(('userID, 'first, 'second)).mapTo(('userID, 'first, 'second) -> ('userID, 'filteredFirst, 'filteredSecond)){
        fields : (String, List[String], List[String]) =>
            val (user, first, second) = fields
            val filterFirst = first.filter{fi : String => isNumeric(fi)}
            var headFirst = ""
            if (!filterFirst.isEmpty){
                headFirst = filterFirst.head
            }

            val filterSecond = second.filter{fi : String => !isNumeric(fi)}
            var headSecond = ""
            if (!filterSecond.isEmpty){
                headSecond = filterSecond.head
            }

            (user, headFirst, headSecond)
    }
            .write(TextLine(out))

    def isNumeric(input: String): Boolean = input.forall(_.isDigit)
}

object ServiceIDCleaner {
    val DataExtractLine: Regex = """(.*)\t(.*)\t(.*)""".r
}



