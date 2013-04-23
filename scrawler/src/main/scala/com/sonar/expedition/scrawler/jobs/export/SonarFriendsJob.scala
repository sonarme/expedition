package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.{TextLine, Args, SequenceFile}
import com.sonar.scalding.cassandra._
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.jobs.{Ddb, DefaultJob}
import com.sonar.dossier.dao.cassandra.ServiceProfileLinkSerializer
import org.codehaus.jackson.map.ObjectMapper
import scala.reflect.BeanProperty

class SonarFriendsJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val rpcHostArg = args("rpcHost")
    val output = args("output")
    val export = args.optional("export").map(_.toBoolean).getOrElse(false)
    if (export) {
        CassandraSource(
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
    } else {
        SequenceFile(output, ('sonarId, 'serviceProfileId)).map(('sonarId, 'serviceProfileId) ->('sonarId, 'serviceProfileId)) {
            in: (String, String) =>
                ("sonarId\u0003" + SonarFriendsJob.om.writeValueAsString(new AttributeValue(in._1)),
                        "friendProfileId\u0003" + SonarFriendsJob.om.writeValueAsString(AValue(in._2)))
        }.groupBy('sonarId) {
            _.mkString('serviceProfileId -> 'out, "\u0002")
        }.write(Ddb(output + "_ddb", ('sonarId, 'out)))
    }

}

object SonarFriendsJob {
    val om = new ObjectMapper
}

case class AValue(@BeanProperty var s: String)