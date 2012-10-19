package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.dossier.dao.cassandra.ServiceProfileLinkSerializer
import com.sonar.expedition.scrawler.util.Tuples

class SonarFriendsExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val rpcHostArg = args("rpcHost")
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        keyspaceName = "dossier",
        columnFamilyName = "SonarFriends",
        scheme = WideRowScheme(keyField = 'sonarIdBuffer,
            nameValueFields = ('splBuffer, 'someBuffer))
    ).mapTo(('sonarIdBuffer, 'splBuffer) ->('sonarId, 'serviceType, 'serviceProfileId)) {
        in: (ByteBuffer, ByteBuffer) =>
            val (sonarIdBuffer, splBuffer) = in
            val sonarId = StringSerializer.get().fromByteBuffer(sonarIdBuffer)
            val spl = ServiceProfileLinkSerializer.fromByteBuffer(splBuffer)
            (sonarId, spl.serviceType, spl.key)
    }.unique(Tuples.SonarFriend).write(SequenceFile(output, Tuples.SonarFriend))
            .limit(180000).write(SequenceFile(output + "_small", Tuples.SonarFriend))
}
