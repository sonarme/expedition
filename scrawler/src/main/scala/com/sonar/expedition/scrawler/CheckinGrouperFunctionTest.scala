package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.CheckinObjects
import java.security.MessageDigest
import cascading.pipe.Pipe
import com.sonar.expedition.scrawler.CheckinGrouperFunction

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouperFunction._
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}

class CheckinGrouperFunctionTest(args: Args) extends Job(args){
    var in = "/tmp/tcheckinData.txt"
    var out = "/tmp/userGroupedCheckins.txt"
    //val groupFuncTest = new CheckinGrouperFunction()

    val pipe1 = TextLine(in).read.project('line)
    val pipe2=groupCheckins(in).write(TextLine(out))
    //val test = TextLine(in).then{
      //  groupFuncTest.groupCheckins( _  )
    //}
    //.write(TextLine(out))

    def groupCheckins(checkinInput : String): RichPipe = {


        val data = TextLine(checkinInput).read.project('line)
                .mapTo('line -> ('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
            line: String => {
                line match {
                    case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                    case _ => ("None","None","None","None","None","None","None","None","None","None")
                }
            }
        }
       .pack[CheckinObjects](('serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude) -> 'checkin)
                .groupBy('userProfileID) {
            group => group.toList[CheckinObjects]('checkin,'checkindata)
        }.map(Fields.ALL -> ('ProfileID, 'venue)){
            fields : (String,List[CheckinObjects]) =>
                val (userid ,checkin) = fields
                val venue = checkin.map(_.getVenueName)
                (userid,venue)
        }.project('ProfileID,'venue)

        data
    }
}

