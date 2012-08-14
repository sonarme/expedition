package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Job, Args, RichPipe}
import com.sonar.expedition.scrawler.util.Haversine

class ReachLoyaltyAnalysis(args: Args) extends Job(args) {

    def findReach(dataAnalyserInput: RichPipe, checkinInput: RichPipe): RichPipe = {

        val joined = checkinInput.joinWithSmaller('keyid -> 'key, dataAnalyserInput)

        val processed = joined.map(('loc, 'homeCentroid, 'workCentroid) -> 'distanceTraveled, 'isHome) {
            fields : (String, String, String) => {
                val (loc, home, work) = fields
                val loclat = loc.split(":").head
                val loclong = loc.split(":").last
                val homelat = home.split(":").head
                val homelong = home.split(":").last
                val worklat = work.split(":").head
                val worklong = work.split(":").last
                val homedist = Haversine.haversine(loclat, loclong, homelat, homelong)
                val workdist = Haversine.haversine(loclat, loclong, worklat, worklong)
                (math.min(homedist, workdist), homedist < workdist)

            }
        }
        processed

    }

//    def findLoyalty()

}
