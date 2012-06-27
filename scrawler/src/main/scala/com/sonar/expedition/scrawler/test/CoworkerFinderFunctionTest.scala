package com.sonar.expedition.scrawler.test

import com.twitter.scalding._
import com.sonar.expedition.scrawler.CoworkerFinderFunction

class CoworkerFinderFunctionTest(args: Args) extends Job(args) {
    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val friendsInput = "/tmp/userGroupedFriends.txt"
    val serviceIdsInput = "/tmp/serviceIds.txt"
    val pipedcoworkers = "/tmp/pipedcoworkers.txt"

    val coworkerFuncTest = new CoworkerFinderFunction(args)

    val serviceProfilePipe = TextLine(serviceProfileInput).read.project('line)
    val friendsPipe = TextLine(friendsInput).read.project('line)
    val serviceIdPipe = TextLine(serviceIdsInput).read.project('line)
    val findCoworkersPipe = coworkerFuncTest.findCoworkers(serviceProfilePipe, friendsPipe, serviceIdPipe).project(('lnoriginalUId, 'friendUserId, 'emp, 'fboriginalUId, 'friendUId, 'emplyer)).write(TextLine(pipedcoworkers))

}
