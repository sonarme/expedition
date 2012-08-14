package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Job, Args, RichPipe}

class ExplorabilityAnalysis(args: Args) extends Job(args) {

    def findExplorability(dataAnalyserInput: RichPipe, checkinInput: RichPipe): RichPipe = {

        val joined = checkinInput.joinWithSmaller('keyid -> 'key, dataAnalyserInput)

        val processed = joined.map(('loc, 'homeCentroid, 'workCentroid) -> 'distanceTraveled) {
            fields : (String, String, String) => {
                val (loc, home, work) = fields
                loc

            }
        }
        processed

    }

}
