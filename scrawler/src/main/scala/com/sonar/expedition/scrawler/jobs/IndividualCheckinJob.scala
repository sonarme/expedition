package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}

class IndividualCheckinJob(args: Args) extends Job(args) with CheckinSource {
    val (checkins, _) = checkinSource(args, false, false)
    val individual = args("keyid")
    checkins.filter('keyid) {
        keyid: String => keyid == individual
    }.write(SequenceFile(args("output")))
}

