package com.sonar.expedition.scrawler.batchjobs

import com.twitter.scalding._
import java.nio.ByteBuffer
import ServiceIdCleaner._
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}

class ServiceIdCleaner(args: Args) extends Job(args) {


    // extracts key data for employerfinder to use. no longer groups
    var inputData = "/tmp/serviceIds.txt"
    var out = "/tmp/serviceIdsCleaned.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('userId, 'oneId, 'twoId)) {
        line: String => {
            line match {
                case DataExtractLine(userId, oneId, twoId) => (userId, oneId, twoId)
                case _ => ("None", "None", "None")
            }
        }
    })
            .groupBy('userId) {
        group => group
                .toList[String]('oneId, 'first).sortBy('oneId)
                .toList[String]('twoId, 'second).sortBy('twoId)
    }
            .project(('userId, 'first, 'second)).mapTo(('userId, 'first, 'second) ->('userId, 'filteredFirst, 'filteredSecond)) {
        fields: (String, List[String], List[String]) =>
            val (user, first, second) = fields
            val filterFirst = first.filter {
                fi: String => isNumeric(fi)
            }
            var headFirst = ""
            if (!filterFirst.isEmpty) {
                headFirst = filterFirst.head
            }

            val filterSecond = second.filter {
                fi: String => !isNumeric(fi)
            }
            var headSecond = ""
            if (!filterSecond.isEmpty) {
                headSecond = filterSecond.head
            }

            (user, headFirst, headSecond)
    }
            .write(TextLine(out))

    def isNumeric(input: String): Boolean = input.forall(_.isDigit)
}

object ServiceIdCleaner {
    val DataExtractLine: Regex = """(.*)\t(.*)\t(.*)""".r
}



