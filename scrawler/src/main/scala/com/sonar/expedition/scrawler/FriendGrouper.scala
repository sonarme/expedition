// package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.FriendObjects

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import FriendGrouper._
import util.matching.Regex
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
        //        var packedData = data.pack[Checkin](('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message) -> 'person)
        //        group => group.toList[Tuple8[String,String,String,String,String,String,String,String]](packedData,'iid)
        //_.sortBy('userProfileId)
        group => group.toList[FriendObjects]('friend,'friendData)
    }.map(Fields.ALL -> ('ProfileId, 'friendName)){
        fields : (String,List[FriendObjects]) =>
            val (userid, friends)    = fields
            val friendName = getFriendName(friends)
            (userid, friendName)
    }.project('ProfileId,'friendName).write(TextLine(out))

    def getFriendName(friends:List[FriendObjects]) : String ={

        friends.map(_.getFriendName).toString()
    }
}


object FriendGrouper {
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r
}



