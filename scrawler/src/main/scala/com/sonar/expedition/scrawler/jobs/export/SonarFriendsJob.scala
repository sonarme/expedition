package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.Args
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.dossier.dao.cassandra.ServiceProfileLinkSerializer

class SonarFriendsJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val rpcHostArg = args("rpcHost")
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        keyspaceName = "dossier",
        columnFamilyName = "SonarFriends",
        scheme = WideRowScheme(keyField = 'rowKeyBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).mapTo(('rowKeyBuffer, 'columnNameBuffer, 'jsondataBuffer) ->('sonarId, 'serviceProfileId)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) =>
            val (rowKeyBuffer, columnNameBuffer, jsondataBuffer) = in
            (StringSerializer.get().fromByteBuffer(rowKeyBuffer), ServiceProfileLinkSerializer.fromByteBuffer(columnNameBuffer).profileId)
    }.write(SequenceFile(output, ('sonarId, 'serviceProfileId)))

}
