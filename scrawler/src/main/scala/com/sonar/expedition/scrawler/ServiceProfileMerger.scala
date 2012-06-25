package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.FriendObjects

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import ServiceProfileMerger._
import util.matching.Regex
import java.security.MessageDigest
import grizzled.slf4j.Logging


class ServiceProfileMerger(args: Args) extends Job(args) {
    val inputData = "/tmp/serviceProfileData.txt"
    val out = "/tmp/mergedServiceProfileData.txt"
    val data = (TextLine(inputData).read.project('line).map(('line) -> ('userProfileID, 'serviceType, 'json)) {
        line: String => {
            line match {
                case DataExtractLine(userProfileID, serviceType, json) => (userProfileID, serviceType, json)
                case _ => ("None","None","None")
            }
        }
    }).groupBy('userProfileID){
        _
            .toList[String]('serviceType -> 'serviceTypelist)
            .toList[String]('json -> 'jsonList)
    }

//        group:GroupBuilder=>
//           group.toList[String]('serviceType -> 'serviceTypelist)
               // .toList[String]('json -> 'jsonList)
            //.toList[String]('json -> 'jsonList) // there can be more fields here
        .project('userProfileID, 'serviceTypelist, 'jsonList).write(TextLine(out))
}


object ServiceProfileMerger {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|4s) : (.*)""".r
}





