package com.sonar.expedition.scrawler.pipes

import java.security.MessageDigest
import com.twitter.scalding.{RichPipe, Args}
import util.matching.Regex
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import com.sonar.expedition.scrawler.clustering._
import org.joda.time.DateTime
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.util.LocationClusterer


trait CheckinInfoPipe extends ScaldingImplicits {

    def findCityofUserFromChkins(chkins: RichPipe): RichPipe = {
        val citypipe = chkins.groupBy(('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle)) {
            _
                    //.mapReduceMap('key->'key1), todo understand mapreducemap api
                    .toList[String]('loc -> 'locList)

        }.mapTo(('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'locList) ->('workco, 'name, 'wrkcity, 'wrktitle, 'fb, 'ln)) {
            fields: (String, String, String, String, String, String, String, List[String]) =>
                val (key, uname, fbid, lnid, worked, city, worktitle, chkinlist) = fields
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
        }.project(('mtphnWorked, 'uname, 'city, 'worktitle, 'fbid, 'lnid))

        citypipe
    }

    def findClusteroidofUserFromChkins(chkins: RichPipe) =
        chkins.groupBy('key) {
            _
                    //.mapReduceMap('key->'key1), todo understand mapreducemap api

                    .toList[String]('loc -> 'locList)

        }.mapTo(('key, 'locList) ->('key, 'centroid)) {
            fields: (String, List[String]) =>
                val (key, locationList) = fields
                val clusterCenter = findCityFromChkins(locationList)
                (key, clusterCenter)
        }


    def findCityFromChkins(chkinlist: List[String]): String =
        if (chkinlist.isEmpty) "0.0:0.0" // TODO: hack
        else {
            val (lat, lng) = LocationClusterer.maxClusterCenter(chkinlist.map {
                s => val Array(lat, lng) = s.split(':')
                (lat.toDouble, lng.toDouble)
            })
            lat + ":" + lng
        }

}
