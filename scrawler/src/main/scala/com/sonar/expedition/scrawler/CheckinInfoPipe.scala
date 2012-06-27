package com.sonar.expedition.scrawler

import com.twitter.scalding.{Job, Args}
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
import CheckinInfoPipe._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class CheckinInfoPipe(args: Args) extends Job(args) {

    def getCheckinsDataPipe(checkinInput: RichPipe): RichPipe = {

        val chkindata = (checkinInput.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng)) {
            line: String => {
                line match {
                    case chkinDataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, tweet) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                    case _ => ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None")
                }
            }
        })
                .project('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng)
                .mapTo(('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng) ->('userProfileID1, 'serviceType1, 'serviceProfileID1, 'serviceCheckinID1, 'venueName1, 'venueAddress1, 'checkinTime1, 'geohash1, 'loc1)) {
            fields: (String, String, String, String, String, String, String, String, String, String) =>
                val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) = fields
                // println("location " +lat + ","+ lng)

                (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat + ":" + lng)

        }.map(('userProfileID1, 'serviceType1, 'serviceProfileID1, 'serviceCheckinID1, 'venueName1, 'venueAddress1, 'checkinTime1, 'geohash1, 'loc1) ->('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'location)) {
            fields: (String, String, String, String, String, String, String, String, String) =>
                val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, loc) = fields
                (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, loc)

        }
                .project('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'location)
                .map(('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'location) ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)) {
            fields: (String, String, String, String, String, String, String, String, String) =>
                val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, loc) = fields
                //val hashedServiceID = md5SumString(serviceID.getBytes("UTF-8"))
                val hashedServiceID = serviceID
                (id, serviceType, hashedServiceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, loc)
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

        chkindata

    }

    def findCityofUserFromChkins(chkins: RichPipe): RichPipe = {
        val citypipe = chkins.groupBy('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle) {
            _
                    //.mapReduceMap('key->'key1), todo understand mapreducemap api
                    .toList[String]('loc -> 'locList)

        }.mapTo(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'locList) ->('workco, 'name, 'wrkcity, 'wrktitle, 'fb, 'ln)) {
            fields: (String, String, String, String, String, String, String, List[String]) =>
                val (key, uname, fbid, lnid, worked, city, worktitle, chkinlist) = fields
                //println("city" + city + chkinlist)
                if (city == "null") {
                    val chkcity = findCityFromChkins(chkinlist)
                    (worked, uname, chkcity, worktitle, fbid, lnid)
                } else {
                    (worked, uname, city, worktitle, fbid, lnid)
                }
        }.mapTo(('workco, 'name, 'wrkcity, 'wrktitle, 'fb, 'ln) ->('worked, 'uname, 'city, 'worktitle, 'fbid, 'lnid)) {
            fields: (String, String, String, String, String, String) =>
                val (worked, uname, city, worktitle, fbid, lnid) = fields
                (worked, uname, city, worktitle, fbid, lnid)
        }.project('worked, 'uname, 'city, 'worktitle, 'fbid, 'lnid)

        citypipe
    }

    def findCityFromChkins(chkinlist: List[String]): String = {
        var centroid = Array(0.toDouble, 0.toDouble)
        chkinlist foreach {
            chkin =>
                centroid(0) += chkin.split(":").firstOption.mkString.toDouble
                centroid(1) += chkin.split(":").lastOption.mkString.toDouble
        }

        val totalPoints = chkinlist.length;
        if (totalPoints == 0) {
            return centroid(0) + ":" + centroid(1);
        }
        centroid(0) = centroid(0) / totalPoints;
        centroid(1) = centroid(1) / totalPoints;
        val km = new KMeans();
        val clusters = 3;
        // chnage no of clustures required
        val chkins: java.util.List[String] = ListBuffer(chkinlist: _*)
        val res = km.clusterKMeans(chkins, clusters)
        return res;

    }

}

object CheckinInfoPipe {
    val chkinDataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}