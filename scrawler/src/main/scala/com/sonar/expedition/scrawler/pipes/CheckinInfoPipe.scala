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

    // todo: mr-dbscan
    def findClusterCenter(chkins: RichPipe) =
        chkins.groupBy('key) {
            _.toList[String]('loc -> 'locList)
        }.flatMap('locList -> 'centroid) {
            locationList: List[String] => findCityFromChkins(locationList)
        }.discard('locList)


    def findCityFromChkins(chkinlist: List[String]) =
        if (chkinlist.isEmpty) None
        else {
            val points = chkinlist.map {
                s => val Array(lat, lng) = s.split(':')
                (lat.toDouble, lng.toDouble)
            }
            LocationClusterer.maxClusterCenter(points) map {
                case (lat, lng) => lat + ":" + lng
            }

        }

}
