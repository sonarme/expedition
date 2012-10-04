package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import com.sonar.expedition.scrawler.util

class IndividualCheckinJob(args: Args) extends Job(args) with CheckinSource {
    val (checkins, _) = checkinSource(args, false, false)
    val Array(serviceType, serviceId) = args("serviceProfileId").split(':')
    val individual = serviceType + ":" + util.CommonFunctions.hashed(serviceId)
    checkins.filter('keyid) {
        keyid: String => keyid == individual
    }.write(SequenceFile(args("output")))
}

