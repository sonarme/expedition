package com.sonar.expedition.scrawler.test

import com.sonar.expedition.scrawler.pipes.FriendGrouperFunction

import com.twitter.scalding._

class FriendGrouperFunctionTest(args: Args) extends Job(args) {
    var in = "/tmp/friendData.txt"
    var out = "/tmp/userGroupedFriends.txt"
    val groupFuncTest = new FriendGrouperFunction(args)

    //TextLine(in).read.project('line).write(TextLine(out))

    val pipe1 = TextLine(in).read.project('line)
    val pipe2 = groupFuncTest.groupFriends(pipe1).write(TextLine(out))
    //val test = TextLine(in).then{
    //  groupFuncTest.groupCheckins( _  )
    //}
    //.write(TextLine(out))


}
