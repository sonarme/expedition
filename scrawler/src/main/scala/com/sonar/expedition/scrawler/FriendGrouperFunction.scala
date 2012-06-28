package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.{CheckinGrouperFunction, CheckinObjects}
import java.security.MessageDigest
import cascading.pipe.{Each, Pipe}
import com.twitter.scalding.TextLine
import cascading.flow.FlowDef

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import com.sonar.expedition.scrawler.FriendGrouperFunction._
import java.nio.ByteBuffer
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}

class FriendGrouperFunction(args : Args) extends Job(args) {

    def groupFriends(input : RichPipe): RichPipe = {


        val data = input.mapTo(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)) {
            line: String => {
                line match {
                    case DataExtractLine(id, other2, serviceId, serviceType, friendName, other) => (id, serviceType, serviceId, friendName)
                    case _ => ("None","None","None","None")
                }
            }
        }
//        .pack[FriendObjects](('serviceType, 'serviceProfileId, 'friendName) -> 'friend).groupBy('userProfileId) {
//            group => group.toList[FriendObjects]('friend,'friendData)
//        }.map(Fields.ALL -> ('ProfileId, 'friendProfileId)){
//            fields : (String,List[FriendObjects]) =>
//                val (userid, friends) = fields
//                val friendProfileId = friends.map(_.getServiceProfileId)
//                (userid, friendProfileId)
//        }
        .project(('userProfileId, 'serviceProfileId)).groupBy('userProfileId) {
            group => group.toList[String]('serviceProfileId, 'serviceProfileIdList)
        }.project('userProfileId, 'serviceProfileIdList)

        data
    }



}

object FriendGrouperFunction {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r
}



