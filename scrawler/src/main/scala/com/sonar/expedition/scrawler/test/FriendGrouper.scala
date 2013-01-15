package com.sonar.expedition.scrawler.test

import cascading.tuple.Fields
import com.sonar.expedition.scrawler.objs.FriendObjects

import com.twitter.scalding._
import FriendGrouper._
import util.matching.Regex
import scala.Some
import scala.Some
import com.twitter.scalding.TextLine
import com.sonar.expedition.scrawler.jobs.DefaultJob

//TODO add service type to each friend serviceid when exporting

// JUST FOR TESTING
class FriendGrouper(args: Args) extends DefaultJob(args) {
    val inputData = args("friendData")
    val out = args("userGroupedFriendsOutput")
    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('userProfileId, 'serviceType, 'serviceProfileId)) {
        line: String => {
            line match {
                case DataExtractLine(id, other2, serviceId, serviceType, _, other) => Some((id, serviceType, serviceId))
                case _ => None
            }
        }
    }).pack[FriendObjects](('serviceType, 'serviceProfileId) -> 'friend).groupBy('userProfileId) {
        group => group.toList[FriendObjects]('friend, 'friendData)
    }.map(Fields.ALL ->('ProfileId, 'friendProfileId)) {
        fields: (String, List[FriendObjects]) =>
            val (userid, friends) = fields
            val friendProfileId = friends.map(_.getServiceProfileId)
            (userid, friendProfileId)
    }.project(('ProfileId, 'friendProfileId)).write(TextLine(out))

    //    This commented section below handles the obfuscation of the userProfileId

    //     .project(Fields.ALL).discard(0).map(Fields.ALL -> ('ProfileId, 'serType, 'serProfileId, 'friName)){
    //        fields : (String, String, String, String) =>
    //            val (userid, serviceType, serviceProfileId, friendName)    = fields
    //            val hashedServiceProfileId = md5SumString(serviceProfileId.getBytes("UTF-8"))
    //            (userid, serviceType, hashedServiceProfileId, friendName)
    //    }.project('ProfileId, 'serType, 'serProfileId, 'friName).write(TextLine(out))
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





