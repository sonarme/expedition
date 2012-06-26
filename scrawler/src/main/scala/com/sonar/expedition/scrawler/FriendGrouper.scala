package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.FriendObjects

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import FriendGrouper._
import util.matching.Regex
import java.security.MessageDigest
import grizzled.slf4j.Logging

     //TODO add service type to each friend serviceid when exporting

class FriendGrouper(args: Args) extends Job(args) {
    val inputData = "/tmp/friendData.txt"
    val out = "/tmp/userGroupedFriends.txt"
    val data = (TextLine(inputData).read.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'friendName)) {
        line: String => {
            line match {
                case DataExtractLine(id, other2, serviceID, serviceType, friendName, other) => (id, serviceType, serviceID, friendName)
                case _ => ("None","None","None","None")
            }
        }
    }).pack[FriendObjects](('serviceType, 'serviceProfileID, 'friendName) -> 'friend).groupBy('userProfileID) {
        group => group.toList[FriendObjects]('friend,'friendData)
    }.map(Fields.ALL -> ('ProfileID, 'friendProfileID)){
        fields : (String,List[FriendObjects]) =>
            val (userid, friends) = fields
            val friendProfileID = friends.map(_.getServiceProfileID)
            (userid, friendProfileID)
    }.project(('ProfileID, 'friendProfileID)).write(TextLine(out))

//    This commented section below handles the obfuscation of the userProfileID

//     .project(Fields.ALL).discard(0).map(Fields.ALL -> ('ProfileID, 'serType, 'serProfileID, 'friName)){
//        fields : (String, String, String, String) =>
//            val (userid, serviceType, serviceProfileID, friendName)    = fields
//            val hashedServiceProfileID = md5SumString(serviceProfileID.getBytes("UTF-8"))
//            (userid, serviceType, hashedServiceProfileID, friendName)
//    }.project('ProfileID, 'serType, 'serProfileID, 'friName).write(TextLine(out))
//    def md5SumString(bytes : Array[Byte]) : String = {
//        val md5 = MessageDigest.getInstance("MD5")
//        md5.reset()
//        md5.update(bytes)
//        md5.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
//    }
}


object FriendGrouper {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r
}





