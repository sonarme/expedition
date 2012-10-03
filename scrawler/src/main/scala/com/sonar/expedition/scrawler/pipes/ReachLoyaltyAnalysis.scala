package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine}
import com.sonar.expedition.scrawler.clustering.KMeansClustering
import grizzled.math.stats._

trait ReachLoyaltyAnalysis extends ScaldingImplicits {

    def findReach(combinedInput: RichPipe) =
        combinedInput.map(('lat, 'lng, 'workCentroid, 'homeCentroid) ->('distanceTraveled, 'isHome)) {
            fields: (Double, Double, String, String) => {
                val (lat, lng, workCentroid, homeCentroid) = fields
                //distance calculation
                val workdist = if (workCentroid == null) -1
                else {
                    val Array(otherLat, otherLng) = workCentroid.split(':')
                    Haversine.haversineInKm(lat, lng, otherLat.toDouble, otherLng.toDouble)
                }
                val homedist = if (homeCentroid == null) -1
                else {
                    val Array(otherLat, otherLng) = homeCentroid.split(':')
                    Haversine.haversineInKm(lat, lng, otherLat.toDouble, otherLng.toDouble)
                }
                (math.min(homedist, workdist), homedist < workdist)

            }
        }.groupBy('venueKey) {
            _.toList[Double](('distanceTraveled) -> ('distancesTraveled))
                    .count('isHome -> 'numHome) {
                x: Boolean => x
            }
                    .count('isHome -> 'numWork) {
                x: Boolean => !x
            }

        }.map('distancesTraveled ->('meanDist, 'stdevDist)) {
            distancesTraveled: List[Double] =>
                val (_, upper) = CommonFunctions.iqrOutlier(distancesTraveled)
                val filtered = distancesTraveled.filter {
                    _ <= upper
                }
                val stdev = if (filtered.size > 1) sampleStdDev(filtered: _*) else 0
                (mean(filtered: _*), if (stdev.isNaN) 0.0 else stdev)
        }.discard('distancesTraveled)


}
