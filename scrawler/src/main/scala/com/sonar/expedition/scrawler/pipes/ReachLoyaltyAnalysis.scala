package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.{CommonFunctions, Haversine}
import grizzled.math.stats._
import com.sonar.dossier.dto.GeodataDTO

trait ReachLoyaltyAnalysis extends ScaldingImplicits {

    def findReach(combinedInput: RichPipe) =
        combinedInput.map(('lat, 'lng, 'work, 'home) ->('distanceTraveled, 'isHome)) {
            fields: (Double, Double, GeodataDTO, GeodataDTO) => {
                val (lat, lng, workCentroid, homeCentroid) = fields
                //distance calculation
                val workdist = if (workCentroid == null) -1
                else {
                    val GeodataDTO(otherLat, otherLng) = workCentroid
                    Haversine.haversineInKm(lat, lng, otherLat, otherLng)
                }
                val homedist = if (homeCentroid == null) -1
                else {
                    val GeodataDTO(otherLat, otherLng) = homeCentroid
                    Haversine.haversineInKm(lat, lng, otherLat, otherLng)
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
