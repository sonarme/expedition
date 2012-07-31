package com.sonar.expedition.scrawler.pipes

import java.security.MessageDigest
import com.twitter.scalding._
import util.matching.Regex
import CheckinInfoPipe._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import com.sonar.expedition.scrawler.clustering.KMeans
import com.sonar.expedition.scrawler.util.CommonFunctions._

class CheckinInfoPipe(args: Args) extends Job(args) {

    def getCheckinsDataPipe(checkinInput: RichPipe): RichPipe = {

        val chkindata = (checkinInput.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng)) {
            line: String => {
                line match {
                    case CheckinExtractLineWithMessages(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                    case _ => ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None")
                }
            }
        })
                .project('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng)
                .map(('lat, 'lng) -> 'loc) {
            fields: (String, String) =>
                val (lat, lng) = fields
                // println("location " +lat + ","+ lng)

                (lat + ":" + lng)

        }
                .discard(('lat, 'lng))
                .project('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'location)
                .map(('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'location) ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)) {
            fields: (String, String, String, String, String, String, String, String, String) =>
                val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, loc) = fields
                val hashedServiceID = serviceID
                (id, serviceType, hashedServiceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, loc)
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

        chkindata

    }

    def getCheckinsDataPipeCollectinLatLon(checkinInput: RichPipe): RichPipe = {

        val chkindata = (checkinInput.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng)) {
            line: String => {
                line match {
                    case CheckinExtractLineWithMessages(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, tweet) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                    case _ => ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None")
                }
            }
        })
                .project('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng)
                .map(('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'lat, 'lng) ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln)) {
            fields: (String, String, String, String, String, String, String, String, String, String) =>
                val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lon) = fields
                val hashedServiceID = hashed(serviceID)
                //val hashedServiceID = serviceID
                (id, serviceType, hashedServiceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lon)
        }
                .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln)
        chkindata

    }

    def findCityofUserFromChkins(chkins: RichPipe): RichPipe = {
        val citypipe = chkins.groupBy('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle) {
            _
                    //.mapReduceMap('key->'key1), todo understand mapreducemap api
                    .toList[String]('loc -> 'locList)

        }.mapTo(('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'locList) ->('workco, 'name, 'wrkcity, 'wrktitle, 'fb, 'ln)) {
            fields: (String, String, String, String, String, String, String, List[String]) =>
                val (key, uname, fbid, lnid, worked, city, worktitle, chkinlist) = fields
                //println("city" + city + chkinlist)
                if (city == "null") {
                    val chkcity = findCityFromChkins(chkinlist)
                    (worked, uname, chkcity, worktitle, fbid, lnid)
                } else {
                    (worked, uname, city, worktitle, fbid, lnid)
                }
        }.mapTo(('workco, 'name, 'wrkcity, 'wrktitle, 'fb, 'ln) ->('mtphnWorked, 'uname, 'city, 'worktitle, 'fbid, 'lnid)) {
            fields: (String, String, String, String, String, String) =>
                val (worked, uname, city, worktitle, fbid, lnid) = fields
                (worked, uname, city, worktitle, fbid, lnid)
        }.project('mtphnWorked, 'uname, 'city, 'worktitle, 'fbid, 'lnid)

        citypipe
    }

    def findClusteroidofUserFromChkins(chkins: RichPipe): RichPipe = {
        val citypipe = chkins.groupBy('key) {
            _
                    //.mapReduceMap('key->'key1), todo understand mapreducemap api
                    .toList[String]('loc -> 'locList)

        }.mapTo(('key, 'locList) ->('key1, 'centroid)) {
            fields: (String, List[String]) =>
                val (key, chkinlist) = fields
                val chkcity = findCityFromChkins(chkinlist)
                (key, chkcity)
        }

        citypipe
    }

    def findCityFromChkins(chkinlist: List[String]): String = {
        var centroid = Array(0.toDouble, 0.toDouble)
        chkinlist foreach {
            chkin =>
                centroid(0) += chkin.split(":").headOption.mkString.toDouble
                centroid(1) += chkin.split(":").lastOption.mkString.toDouble
        }

        val totalPoints = chkinlist.length
        if (totalPoints == 0) {
            return centroid(0) + ":" + centroid(1)
        }
        centroid(0) = centroid(0) / totalPoints
        centroid(1) = centroid(1) / totalPoints
        val km = new KMeans()
        val clusters = 3
        // chnage no of clustures required
        val chkins: java.util.List[String] = ListBuffer(chkinlist: _*)
        km.clusterKMeans(chkins, clusters)

    }

}

object CheckinInfoPipe {

}