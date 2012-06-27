package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO, Checkin}
import java.security.MessageDigest
import cascading.pipe.{Each, Pipe}
import com.twitter.scalding.TextLine
import cascading.flow.FlowDef
import com.twitter.scalding._
import java.nio.ByteBuffer
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import java.util.Calendar
import FriendInfoPipe._

import com.twitter.scalding.{Job, Args}

class FriendInfoPipe(args: Args) extends Job(args) {
    def friendsDataPipe(checkinInput: RichPipe): RichPipe = {
        val friends = (checkinInput.project('line).map(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)) {
            line: String => {
                line match {
                    case DataExtractLineFriend(id, other2, serviceID, serviceType, friendName) => (id, serviceType, serviceID, friendName)
                    case NullNameExtractLine(id, other2, serviceID, serviceType, friendName) => (id, serviceType, serviceID, friendName)
                    case _ => ("None", "None", "None", "None")
                }
            }
        }).project('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)

//        Use the code below when md5 hashing is needed

//                .project(Fields.ALL).discard(0).map(Fields.ALL ->('key, 'serType, 'serProfileId, 'friName)) {
//            fields: (String, String, String, String) =>
//                val (userid, serviceType, serviceProfileId, friendName) = fields
//                //val hashedServiceProfileId = md5SumString(serviceProfileId.getBytes("UTF-8"))
//                val serProfileId = serviceProfileId
//                (userid, serviceType, serProfileId, friendName)
//        }.project('key, 'serType, 'serProfileId, 'friName)

        friends

    }
}

object FriendInfoPipe {
    val DataExtractLineFriend: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo.*""".r
    val NullNameExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":(null),"photo.*""".r

}
