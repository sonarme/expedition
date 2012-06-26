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


        val data = input.mapTo(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'friendName)) {
            line: String => {
                line match {
                    case DataExtractLine(id, other2, serviceID, serviceType, friendName, other) => (id, serviceType, serviceID, friendName)
                    case _ => ("None","None","None","None")
                }
            }
        }
        .pack[FriendObjects](('serviceType, 'serviceProfileID, 'friendName) -> 'friend).groupBy('userProfileID) {
            group => group.toList[FriendObjects]('friend,'friendData)
        }.map(Fields.ALL -> ('ProfileID, 'friendProfileID)){
            fields : (String,List[FriendObjects]) =>
                val (userid, friends) = fields
                val friendProfileID = friends.map(_.getServiceProfileID)
                (userid, friendProfileID)
        }.project(('ProfileID, 'friendProfileID))

        data
    }

}

object FriendGrouperFunction {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r
}



