package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import me.prettyprint.cassandra.serializers.StringSerializer
import cascading.tuple.Fields

class ServiceProfileExportRawJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "ServiceProfile",
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameField = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    profiles
            .flatMapTo(('userProfileIdBuffer, 'columnNameBuffer, 'jsondataBuffer) ->('userProfileId, 'serviceType, 'json)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) =>
            val (userProfileIdBuffer, columnNameBuffer, jsondataBuffer) = in
            if (jsondataBuffer == null || !jsondataBuffer.hasRemaining) None
            else {
                val serviceTypeString = StringSerializer.get().fromByteBuffer(columnNameBuffer).split(':').last
                if (serviceTypeString == "link") None
                else
                    Some((StringSerializer.get().fromByteBuffer(userProfileIdBuffer), serviceTypeString, StringSerializer.get().fromByteBuffer(jsondataBuffer)))
            }
    }.write(SequenceFile(output, Fields.ALL))
            .limit(180000).write(SequenceFile(output + "_small", Fields.ALL))
}
