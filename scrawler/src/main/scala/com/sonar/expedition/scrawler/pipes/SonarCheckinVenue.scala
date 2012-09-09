package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util._


trait SonarCheckinVenue extends RealSocialGraph {

    // input of unfiltered checkins
    // ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'dayOfWeek, 'hour)
    def getCheckinVenue(checkins: RichPipe, friends: RichPipe, serviceProfiles: RichPipe): RichPipe = {

        val default = ("", "", 0.0, 0.0, 0)
        val cutoff = 3

        val sonarPipe = checkins
                .filter('serType) {
            serType: String => serType.equals("sonar")
        }
                .unique(('keyid, 'loc))
                .rename(('keyid, 'loc) ->('keyid2, 'loc2))

        val nonSonarPipe = checkins
                .filter('serType) {
            serType: String => !serType.equals("sonar")
        }
                .map('keyid -> 'weight) {
            key: String => 1.0
        }

        // ('uId, 'friendKey, 'uname, 'uname2, 'size)
        val mergedFriends =
            friendsNearbyByFriends(friends, checkins, serviceProfiles)
                    .project(('uId, 'friendKey))

        val friendCheckins = checkins
                .filter('serType) {
            serType: String => !serType.equals("sonar")
        }
                .joinWithSmaller('keyid -> 'friendKey, mergedFriends)
                .discard(('keyid, 'friendKey))
                .rename('uId -> 'keyid)
                .map('keyid -> 'weight) {
            key: String => 0.25
        }
                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'dayOfWeek, 'hour, 'weight))

        val namePipe = nonSonarPipe
                .++(friendCheckins)
                .groupBy(('keyid, 'venName, 'loc)) {
            _.toList[Double]('weight -> 'weightList)
        }
                .map('weightList -> 'weight) {
            list: List[Double] => {
                val sumSq = list.foldLeft[Double](0.0)((a, b) => a + (b * b))
                Math.sqrt(sumSq)
            }
        }
                .discard('weightList)

        val joinedPipe = namePipe.joinWithSmaller(('keyid -> 'keyid2), sonarPipe)
                .groupBy(('keyid, 'loc2)) {
            _.toList[(String, String, String)](('venName, 'loc, 'weight) -> 'checkinList)
        }

        val output = joinedPipe.flatMap('checkinList ->('venName, 'loc, 'weight)) {
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
                val dist = Haversine.haversineInKm(sonarLat, sonarLng, lat, lng)

                dist
            }
        }
                .filter('score) {
            score: Double => score < cutoff
        }
                .map(('score, 'weight) -> 'adjustedScore) {
            fields: (Double, Double) => {
                val (score, weight) = fields
                -score / weight
            }
        }
                .groupBy(('keyid, 'loc2)) {
            _.toList[(String, String, Double, Double, Double)](('loc, 'venName, 'score, 'adjustedScore, 'weight) -> 'infoList).sortBy('adjustedScore)
        }
                .map('infoList -> 'topThree) {
            list: List[(String, String, Double, Double, Int)] => {
                val newList = List[(String, String, Double, Double, Int)]()
                val one = list.head
                val two = list.tail.headOption.getOrElse(default)
                if (two._1.equals(""))
                    newList.::(default).::(two).::(one)
                else
                    newList.::(list.tail.tail.headOption.getOrElse(default)).::(two).::(one)
            }
        }
                .discard('infoList)
                .flatMap('topThree ->('loc, 'venName, 'score, 'adjustedScore, 'weight)) {
            list: List[(String, String, Double, Double, Double)] => {
                list
            }
        }
                .discard(('topThree, 'score))
                .filter('loc) {
            loc: String => !loc.isEmpty
        }

        output

    }

}
