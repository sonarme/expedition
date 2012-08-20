package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Job, Args, TextLine}
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction

class CheckinParse(args: Args) extends Job(args) {

    val checkinGroup = new CheckinGrouperFunction(args)

    val checkins = checkinGroup.unfilteredCheckinsLatLon(TextLine(args.getOrElse("checkin", "/tmp/checkinDatatest.txt")))

    checkins
            .groupAll {
        _.size
    }
            .write(TextLine("/tmp/CheckinParse.txt"))
}
