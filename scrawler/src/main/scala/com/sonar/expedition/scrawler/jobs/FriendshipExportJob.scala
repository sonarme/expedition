package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.util.CommonFunctions._

class FriendshipExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "Friendship",
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameField = ('columnNameBuffer, 'jsondataBuffer))
    ).mapTo(('userProfileIdBuffer, 'columnNameBuffer, 'jsondataBuffer) ->('userProfileId, 'serviceType, 'serviceProfileId)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) =>
            val (userProfileIdBuffer, columnNameBuffer, jsondataBuffer) = in
            val userProfileId = StringSerializer.get().fromByteBuffer(userProfileIdBuffer)
            val Array(friendUserProfileId, serviceType, friendServiceProfileId) = StringSerializer.get().fromByteBuffer(columnNameBuffer).split(':')
            (userProfileId, serviceType, hashed(friendServiceProfileId))
    }.unique(FriendTuple).write(SequenceFile(output, FriendTuple))
            .limit(180000).write(SequenceFile(output + "_small", FriendTuple))
}
