package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.jobs.DefaultJob

class FriendshipExportJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val rpcHostArg = args("rpcHost")
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        keyspaceName = "dossier",
        columnFamilyName = "Friendship",
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).flatMapTo(('userProfileIdBuffer, 'columnNameBuffer, 'jsondataBuffer) ->('userProfileId, 'serviceType, 'serviceProfileId)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) =>
            val (userProfileIdBuffer, columnNameBuffer, jsondataBuffer) = in
            val userProfileId = StringSerializer.get().fromByteBuffer(userProfileIdBuffer)
            StringSerializer.get().fromByteBuffer(columnNameBuffer).split(":", 3) match {
                case Array(friendUserProfileId, serviceType, friendServiceProfileId) =>
                    Some(userProfileId, serviceType, hashed(friendServiceProfileId))
                case _ => None
            }
    }.unique(FriendTuple).write(SequenceFile(output, FriendTuple))
            .limit(180000).write(SequenceFile(output + "_small", FriendTuple))
}
