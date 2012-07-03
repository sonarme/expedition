package com.sonar.expedition.scrawler


import cascading.tuple.Fields
import com.sonar.expedition.scrawler.{CollectCheckins, CheckinInfoPipe, CheckinTimeFilter}
import com.twitter.scalding.{Job, Args, TextLine}
import cascading.tuple.Fields
import com.twitter.scalding.{GroupBuilder, TextLine, Job, Args}
import util.matching.Regex
import CollectCheckins._

import java.util._

class CheckinsDataTest (args: Args) extends Job(args) {

    val chkininputData = TextLine("/tmp/checkinData.txt")
    val output = TextLine("/tmp/output4.txt")
    //val output1 = TextLine("/tmp/output2.txt")
    //val output2 = TextLine("/tmp/output3.txt")

    val chkins = new CheckinInfoPipe(args)



    val chkres1 = chkins.getCheckinsDataPipeCollectinLatLon(chkininputData.read).project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln)
            .groupBy(Fields.ALL) {
        _.sortBy('chknTime)
    }
    .filter('venName) {
        venue: String => (venue.startsWith("Ippudo") || venue.startsWith("Totto") || venue.startsWith("momofuku") || venue.startsWith("Bobby Van"))
    }
    .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln)
    .write(output)
}
