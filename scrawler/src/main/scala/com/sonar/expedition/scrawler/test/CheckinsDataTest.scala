package com.sonar.expedition.scrawler.test

import com.sonar.expedition.scrawler.pipes.CheckinInfoPipe
import cascading.tuple.Fields
import com.twitter.scalding.{TextLine, Job, Args}

class CheckinsDataTest(args: Args) extends Job(args) {

    val chkininputData = TextLine("/tmp/checkinData.txt")
    val output = TextLine("/tmp/output4.txt")
    //val output1 = TextLine("/tmp/output2.txt")
    //val output2 = TextLine("/tmp/output3.txt")

    val chkins = new CheckinInfoPipe(args)


    val chkres1 = chkins.getCheckinsDataPipeCollectinLatLon(chkininputData.read).project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln))
            .groupBy(Fields.ALL) {
        _.sortBy('chknTime)
    }
            .filter('venName) {
        venue: String => (venue.startsWith("Ippudo") || venue.startsWith("Totto") || venue.startsWith("momofuku") || venue.startsWith("Bobby Van"))
    }
            .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln))
            .write(output)
}
