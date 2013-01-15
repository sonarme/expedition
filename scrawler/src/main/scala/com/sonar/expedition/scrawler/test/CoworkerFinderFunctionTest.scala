package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.pipes.CoworkerFinderFunction
import com.twitter.scalding.Job
import com.sonar.expedition.scrawler.jobs.DefaultJob

class CoworkerFinderFunctionTest(args: Args) extends DefaultJob(args) {
    /*
    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val friendsInput = "/tmp/userGroupedFriends.txt"
    val serviceIdsInput = "/tmp/serviceIds.txt"
    val checkinsInput = "/tmp/checkinDatatest.txt"
    val pipedcoworkers = "/tmp/pipedcoworkers.txt"
    val coworkerCheckins = "/tmp/coworkerCheckins.txt"

    val coworkerFuncTest = new CoworkerFinderFunction(args)

    val serviceProfilePipe = TextLine(serviceProfileInput).read.project('line)
    val friendsPipe = TextLine(friendsInput).read.project('line)
    val serviceIdPipe = TextLine(serviceIdsInput).read.project('line)
    val checkinsPipe = TextLine(checkinsInput).read.project('line)
    val findCoworkersPipe = coworkerFuncTest.findCoworkerCheckins(serviceProfilePipe, friendsPipe, serviceIdPipe, checkinsPipe)
            .project(('keyid, 'originalUId, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))
            .write(TextLine(coworkerCheckins))
    */

}
