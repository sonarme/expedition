package com.sonar.expedition.scrawler

import com.twitter.scalding._

class CoworkerFinderFunctionTest(args: Args) extends Job(args){
    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val friendsInput = "/tmp/userGroupedFriends.txt"
    val serviceIdsInput = "/tmp/serviceIds.txt"
    val cowrkerFuncTest = new CoworkerFinderFunction(args)

    //TextLine(in).read.project('line).write(TextLine(out))

    val serviceProfilePipe = TextLine(serviceProfileInput).read.project('line)
    val friendsPipe = TextLine(friendsInput).read.project('line)
    val serviceIdPipe = TextLine(serviceIdsInput).read.project('line)
    val findCoworkersPipe = cowrkerFuncTest.findCoworkers(serviceProfilePipe, friendsPipe, serviceIdPipe).project('lnoriginalUId, 'friendUserId, 'emp, 'fboriginalUId, 'friendUId, 'emplyer).write(TextLine(out))
    //val test = TextLine(in).then{
    //  groupFuncTest.groupCheckins( _  )
    //}
    //.write(TextLine(out))


    val out = "/tmp/pipedcoworkers.txt"
//    val temp = "/tmp/tempcoworkerGroups.txt"
//    val fbfriends = "/tmp/fbfriends.txt"
//    val lnfriends = "/tmp/lnfriends.txt"
//    val allfriends = "/tmp/allfriends.txt"
//    val EandF = "/tmp/employerAndFriends.txt"
//    val lncowrker = "/tmp/lncowrker.txt"
//    val fbcowrker = "/tmp/fbcowrker.txt"

}
