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


class FriendGrouper(args: Args) extends Job(args) {
    var inputData = "/tmp/friendData.txt"
    var out = "/tmp/userGroupedFriends.txt"
    //   logger.debug(checkin.getUserProfileId() + "::" + checkin.getServiceType() + "::" + checkin.getServiceProfileId() + "::" + checkin.getServiceCheckinId() + "::" + checkin.getVenueName() + "::" + checkin.getVenueAddress() + "::" + checkin.getCheckinTime() + "::" + checkin.getGeohash() + "::" + checkin.getLatitude() + "::" + checkin.getLongitude() + "::" + checkin.getMessage())
    var data = (TextLine(inputData).read.project('line).map(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)) {
        line: String => {
            line match {
                case DataExtractLine(id, other2, serviceID, serviceType, friendName, other) => (id, serviceType, serviceID, friendName)
                case _ => ("None","None","None","None")
            }
        }
    }).pack[FriendObjects](('serviceType, 'serviceProfileId, 'friendName) -> 'friend).groupBy('userProfileId) {
        group => group.toList[FriendObjects]('friend,'friendData)
    }.map(Fields.ALL -> ('ProfileId, 'serviceType, 'serviceProfileId, 'friendName)){
        fields : (String,List[FriendObjects]) =>
            val (userid, friends)    = fields
            val friendName = getFriendName(friends)
            (userid, serviceType, serviceProfileId, friendName)
    }.project('ProfileId, 'friendName).write(TextLine(out))

//    This commented section below handles the obfuscation of the userProfileID

     /*.project(Fields.ALL).discard(0).map(Fields.ALL -> ('ProfileId, 'serType, 'serProfileId, 'friName)){
        fields : (String, String, String, String) =>
            val (userid, serviceType, serviceProfileId, friendName)    = fields
            val hashedServiceProfileId = md5SumString(serviceProfileId.getBytes("UTF-8"))
            (userid, serviceType, hashedServiceProfileId, friendName)
    }.project('ProfileId, 'serType, 'serProfileId, 'friName).write(TextLine(out))*/

    def getFriendName(friends:List[FriendObjects]) : String ={

        friends.map(_.getFriendName).toString()
    }

    def getServiceType(friends:List[FriendObjects]) : String ={

        friends.map(_.getServiceType).toString()
    }

    def getServiceProfileId(friends:List[FriendObjects]) : String ={

        friends.map(_.getServiceProfileId).toString()
    }

    def md5SumString(bytes : Array[Byte]) : String = {
        val md5 = MessageDigest.getInstance("MD5")
        md5.reset()
        md5.update(bytes)
        md5.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
    }
}


object FriendGrouper {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r
}





