package com.sonar.expedition.scrawler.test

import com.twitter.scalding._
import com.sonar.expedition.scrawler.CoworkerFinderFunction

class CoworkerFinderFunctionTest(args: Args) extends Job(args) {
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
    val findCoworkersPipe = coworkerFuncTest.findCoworkerCheckins(serviceProfilePipe, friendsPipe, serviceIdPipe, checkinsPipe).project(('lnoriginalUId, 'friendUserId, 'emp, 'venName, 'loc, 'fboriginalUId, 'friendUId, 'emplyer, 'fbvenName, 'fbloc)).write(TextLine(coworkerCheckins))

}
