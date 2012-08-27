package com.sonar.expedition.scrawler.test

import com.sonar.expedition.scrawler.pipes.FriendGrouperFunction
import com.twitter.scalding.{TextLine, Args}
import com.twitter.scalding.Job


class FriendGrouperFunctionTest(args: Args) extends Job(args) with FriendGrouperFunction {
    var in = args("friendData")
    var out = args("userGroupedFriends")

    //TextLine(in).read.project('line).write(TextLine(out))

    val pipe1 = TextLine(in).read.project('line)
    val pipe2 = groupFriends(pipe1).write(TextLine(out))
    //val test = TextLine(in).then{
    //  groupFuncTest.groupCheckins( _  )
    //}
    //.write(TextLine(out))


}
