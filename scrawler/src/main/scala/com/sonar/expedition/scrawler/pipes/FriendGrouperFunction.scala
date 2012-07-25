package com.sonar.expedition.scrawler.pipes


import java.security.MessageDigest

import com.sonar.expedition.scrawler.pipes.FriendGrouperFunction._

import com.twitter.scalding._
import util.matching.Regex

class FriendGrouperFunction(args: Args) extends Job(args) {

    def groupFriends(input: RichPipe): RichPipe = {


        val data = input.mapTo(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)) {
            line: String => {
                line match {
                    // change when we use prod data
                    case DataExtractLine(id, other2, serviceId, serviceType, friendName, other) => (id, serviceType, serviceId, friendName)
                    case _ => ("None", "None", "None", "None")
                }
            }
        }
                //                .map('serviceProfileId -> 'hashedId) {
                //            id: String =>
                //                val hashedServiceId = md5SumString(id.getBytes("UTF-8"))
                //                hashedServiceId
                //        }
                //                .discard('serviceProfileId).rename('hashedId -> 'serviceProfileId)
                //                .pack[FriendObjects](('serviceType, 'serviceProfileId, 'friendName) -> 'friend).groupBy('userProfileId) {
                //            group => group.toList[FriendObjects]('friend,'friendData)
                //        }.map(Fields.ALL -> ('ProfileId, 'friendProfileId)){
                //            fields : (String,List[FriendObjects]) =>
                //                val (userid, friends) = fields
                //                val friendProfileId = friends.map(_.getServiceProfileId)
                //                (userid, friendProfileId)
                //        }
                //        .project(('userProfileId, 'serviceProfileId)).groupBy('userProfileId) {
                //            group => group.toList[String]('serviceProfileId, 'serviceProfileIdList)
                //        }.project(('userProfileId, 'serviceProfileIdList))

                .project('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)

        data
    }

    def md5SumString(bytes: Array[Byte]): String = {
        val md5 = MessageDigest.getInstance("MD5")
        md5.reset()
        md5.update(bytes)
        md5.digest().map(0xFF & _).map {
            "%02x".format(_)
        }.foldLeft("") {
            _ + _
        }
    }

}

object FriendGrouperFunction {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r

    // to be used for prod data
    val NewDataExtractLine: Regex = """(.*)::(.*)::(.*)::(.*)""".r
}



