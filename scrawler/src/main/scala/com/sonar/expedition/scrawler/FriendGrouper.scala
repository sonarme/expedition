package com.sonar.expedition.scrawler

import cascading.tuple.Fields


import com.twitter.scalding._
import java.nio.ByteBuffer
import FriendGrouper._
import util.matching.Regex
import java.security.MessageDigest
import grizzled.slf4j.Logging

     //TODO add service type to each friend serviceid when exporting

class FriendGrouper(args: Args) extends Job(args) {
    var inputData = "/tmp/friendData.txt"
    var out = "/tmp/userGroupedFriends.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'friendName)) {
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
            val friendProfileID = friends.map(_.getServiceProfileId)
            (userid, friendProfileID)
    }.project('ProfileID, 'friendProfileID).write(TextLine(out))

}


object FriendGrouper {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r
}





