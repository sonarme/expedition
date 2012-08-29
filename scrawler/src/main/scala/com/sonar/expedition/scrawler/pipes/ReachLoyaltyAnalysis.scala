package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.Haversine


trait ReachLoyaltyAnalysis extends ScaldingImplicits {

    def findReach(combinedInput: RichPipe): RichPipe = {

        val processed = combinedInput.map(('loc, 'homeCentroid, 'workCentroid) ->('distanceTraveled, 'isHome)) {
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
        }

        val stats = processed.groupBy('venueKey) {
            _.sizeAveStdev('distanceTraveled ->('count, 'meanDist, 'stdevDistRaw))
                    .count('isHome -> 'numHome) {
                x: Boolean => x
            }
                    .count('isHome -> 'numWork) {
                x: Boolean => !x
            }
                    .max('loc)
        }
                .map(('loc) ->('lat, 'lng)) {
            fields: (String) =>
                val loc = fields
                val lat = loc.split(":").head
                val long = loc.split(":").last
                (lat, long)
        }
                .map('stdevDistRaw -> 'stdevDist) {
            stdev: Double => {
                if (stdev.toString.equals("NaN"))
                    0.0
                else
                    stdev
            }
        }

        stats

    }

    def findLoyalty(combinedInput: RichPipe): RichPipe = {

        val processed = combinedInput.groupBy('keyid, 'venueKey) {
            _.size('visits)
        }
                .map('visits -> 'loyalty) {
            size: Int => {
                if (size == 1)
                    "Passers-By"
                else if (size <= 3)
                    "Regulars"
                else
                    "Addicts"

            }
        }

        val stats = processed.groupBy('venueKey, 'loyalty) {
            _.size('customers)
                    .sum('visits -> 'visitsType)
        }

        stats
    }

}
