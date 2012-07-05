package com.sonar.expedition.scrawler.test

import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction

import com.twitter.scalding._

class CheckinGrouperFunctionTest(args: Args) extends Job(args) {
    var in = "/tmp/checkinDatatest.txt"
    var out = "/tmp/userGroupedCheckins.txt"
    val groupFuncTest = new CheckinGrouperFunction(args)

    //TextLine(in).read.project('line).write(TextLine(out))

    val pipe1 = TextLine(in).read.project('line)
    val pipe2 = groupFuncTest.groupCheckins(pipe1).write(TextLine(out))
    //val test = TextLine(in).then{
    //  groupFuncTest.groupCheckins( _  )
    //}
    //.write(TextLine(out))


}
