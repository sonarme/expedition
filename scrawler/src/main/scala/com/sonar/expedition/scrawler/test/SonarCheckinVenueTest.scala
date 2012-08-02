package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.expedition.scrawler.util.CommonFunctions._

/*
inputs : prod exports
--checkinData

output : code with matched friends in real social graph, will have ('keyid, 'keyid2, 'uname, 'uname2, 'size)
--output

com.sonar.expedition.scrawler.test.RealSocialGraphTest --local --checkinData "/data/checkinData.txt"
--output "/tmp/matchedVenues.txt"

 */

class SonarCheckinVenueTest(args: Args) extends Job(args) {

    val checkinsInput = args("checkinData")
    val matchedVenues = args("output")

    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val venueFinder = new SonarCheckinVenue(args)

    val chkindata = checkinGrouperPipe.unfilteredCheckins(TextLine(checkinsInput).read)

    val sonarVenues = venueFinder.getCheckinVenue(chkindata)
            .write(TextLine(matchedVenues))


}