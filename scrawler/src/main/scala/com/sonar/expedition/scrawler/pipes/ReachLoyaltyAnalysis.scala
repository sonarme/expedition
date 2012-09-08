package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.Haversine


trait ReachLoyaltyAnalysis extends ScaldingImplicits {

    def findReach(combinedInput: RichPipe) =
        combinedInput.map(('loc, 'homeCentroid, 'workCentroid) ->('distanceTraveled, 'isHome)) {
            fields: (String, String, String) => {
                val (loc, home, work) = fields
                val loclat = loc.split(":").head.toDouble
                val loclong = loc.split(":").last.toDouble
                val homelat = home.split(":").head.toDouble
                val homelong = home.split(":").last.toDouble
                val worklat = work.split(":").head.toDouble
                val worklong = work.split(":").last.toDouble
                val homedist = Haversine.haversine(loclat, loclong, homelat, homelong)
                val workdist = Haversine.haversine(loclat, loclong, worklat, worklong)
                (math.min(homedist, workdist), homedist < workdist)

            }
        }.groupBy('venueKey) {

            _.sizeAveStdev('distanceTraveled ->('count, 'meanDist, 'stdevDist))
                    .count('isHome -> 'numHome) {
                x: Boolean => x
            }
                    .count('isHome -> 'numWork) {
                x: Boolean => !x
            }

        }.map('stdevDist -> 'stdevDist) {
            stdev: Double => if (stdev.isNaN) 0.0 else stdev
        }


}
