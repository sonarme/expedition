package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouper._
import util.matching.Regex
import grizzled.slf4j.Logging


class CheckinGrouper(args: Args) extends Job(args) {

    var inputData = "/tmp/checkinData.txt"
    var out = "/tmp/userGroupedCheckins.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('id, 'checkindata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, checkinData) => (userProfileId, checkinData)
                case _ => ("1",line)
            }
        }
    }).groupBy('id) {

        group => group.toList[String]('checkindata,'iid)
    }.write(TextLine(out))

}


object CheckinGrouper {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)""".r
}



