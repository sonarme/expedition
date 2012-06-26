package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.CheckinObjects
import java.security.MessageDigest
import cascading.pipe.Pipe
import com.sonar.expedition.scrawler.FriendGrouperFunction

import com.twitter.scalding._
import java.nio.ByteBuffer
import FriendGrouperFunction._
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}

class FriendGrouperFunctionTest(args: Args) extends Job(args){
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
