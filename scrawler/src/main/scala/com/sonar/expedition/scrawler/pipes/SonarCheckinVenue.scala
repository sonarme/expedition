package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._

class SonarCheckinVenue(args: Args) extends Job(args) {

    // input of unfiltered checkins
    // ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'dayOfWeek, 'hour)
    def getCheckinVenue(checkins: RichPipe): RichPipe = {

        val sonarPipe = checkins
                .filter('serType) {
            serType: String => serType.equals("sonar")
        }
                .project(('keyid, 'loc))
                .rename(('keyid, 'loc) -> ('keyid2, 'loc2))

        val joinedPipe = checkins.joinWithSmaller(('keyid -> 'keyid2), sonarPipe)
                .groupBy(('keyid, 'loc2)) {
            _.toList[(String, String)](('venName, 'loc) -> 'checkinList)
        }

//        val output = joinedPipe.flatMap(('loc2, 'checkinList) -> ('venName, 'loc, 'score)) {
//
//        }

        joinedPipe

    }

}
