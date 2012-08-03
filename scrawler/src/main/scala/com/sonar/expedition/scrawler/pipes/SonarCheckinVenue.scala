package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import com.sonar.expedition.scrawler.util._

class SonarCheckinVenue(args: Args) extends Job(args) {

    // input of unfiltered checkins
    // ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'dayOfWeek, 'hour)
    def getCheckinVenue(checkins: RichPipe): RichPipe = {

        val default = ("", "", 0.0, 0.0, 0)
        val cutoff = 3
        val havver = new Haversine

        val sonarPipe = checkins
                .filter('serType) {
            serType: String => serType.equals("sonar")
        }
                .unique(('keyid, 'loc))
                .rename(('keyid, 'loc) -> ('keyid2, 'loc2))

        val namePipe = checkins
                .filter('serType) {
            serType: String => !serType.equals("sonar")
        }
//                .filter('venName) {
//            venName: String => !venName.equals("")
//        }
                .groupBy(('keyid, 'venName, 'loc)) {
            _.size('count)
        }

        val joinedPipe = namePipe.joinWithSmaller(('keyid -> 'keyid2), sonarPipe)
                .groupBy(('keyid, 'loc2)) {
            _.toList[(String, String, String)](('venName, 'loc, 'count) -> 'checkinList)
        }

        val output = joinedPipe.flatMap('checkinList -> ('venName, 'loc, 'count)) {
                list: List[(String, String, String)] => list
        }
                .discard('checkinList)
                .map(('loc2, 'loc) -> 'score) {
            fields: (String, String) => {
                val (sonarLoc, loc) = fields
                val sonarLat = sonarLoc.split(":").head.toDouble
                val sonarLng = sonarLoc.split(":").last.toDouble
                val lat = loc.split(":").head.toDouble
                val lng = loc.split(":").last.toDouble
                val dist = havver.haversine(sonarLat, sonarLng, lat, lng)

                dist
            }
        }
                .filter('score) {
            score: Double => score < cutoff
        }
                .map(('score, 'count) -> 'adjustedScore) {
            fields: (Double, Int) => {
                val (score, count) = fields
                -score / scala.math.sqrt(count)
            }
        }
                .groupBy(('keyid, 'loc2)) {
            _.toList[(String, String, Double, Double, Int)](('loc, 'venName, 'score, 'adjustedScore, 'count) -> 'infoList).sortBy('adjustedScore)
        }
                .map('infoList -> 'topTwo) {
            list: List[(String, String, Double, Double, Int)] => {
                val newList = List[(String, String, Double, Double, Int)]()
                newList.::(list.tail.headOption.getOrElse(default)).::(list.head)
            }
        }
                .discard('infoList)
                .flatMap('topTwo -> ('loc, 'venName, 'score, 'adjustedScore, 'count)) {
            list: List[(String, String, Double, Double, Int)] => {
                list
            }
        }
                .discard(('topTwo, 'score))
                .filter('loc) {
            loc: String => !loc.isEmpty
        }

        output

    }

}
