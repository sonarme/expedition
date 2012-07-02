//package com.sonar.expedition.scrawler
//
//import cascading.tuple.Fields
//import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
//import com.sonar.dossier.dto.ServiceProfileDTO
//import com.sonar.expedition.scrawler.FriendObjects
//
////import com.sonar.expedition.scrawler.MeetupCrawler
//import com.twitter.scalding._
//import java.nio.ByteBuffer
//import ServiceProfileMerger._
//import util.matching.Regex
//import java.security.MessageDigest
//import grizzled.slf4j.Logging
//
//
//class ServiceProfileMerger(args: Args) extends Job(args) {
//    val serviceProfileInput = "/tmp/serviceProfileData.txt"
//    val checkinInput = "/tmp/checkinData.txt"
//    val friendsInput = "/tmp/friendsData.txt"
//    val out = "/tmp/mergedServiceProfileData.txt"
//    val serviceProfileData = (TextLine(inputData).read.project('line).map(('line) -> ('userProfileId, 'serviceType, 'json)) {
//        line: String => {
//            line match {
//                case DataExtractLine(userProfileId, serviceType, json) => (userProfileId, serviceType, json)
//                case _ => ("None","None","None")
//            }
//        }
//    }).groupBy('userProfileId){
//        _
//            .toList[String]('serviceType -> 'serviceTypelist)
//            .toList[String]('json -> 'jsonList)
//    }.project('userProfileId, 'serviceTypelist, 'jsonList)
//
//    val friendsData = (TextLine(inputData).read.project('line).map(('line) -> ('userProfileId, 'serviceType, 'json)) {
//        line: String => {
//            line match {
//                case DataExtractLine(userProfileId, serviceType, json) => (userProfileId, serviceType, json)
//                case _ => ("None","None","None")
//            }
//        }
//    }).groupBy('userProfileId){
//        _
//                .toList[String]('serviceType -> 'serviceTypelist)
//                .toList[String]('json -> 'jsonList)
//    }.project('userProfileId, 'serviceTypelist, 'jsonList)
//}
//
//
//object ServiceProfileMerger {
//    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|4s) : (.*)""".r
//}
//
//
//
//
//
