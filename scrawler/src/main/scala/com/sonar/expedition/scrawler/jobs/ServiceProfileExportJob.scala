package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra._
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import com.sonar.expedition.scrawler.util.CommonFunctions
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import scala.Some
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import scala.Some
import com.sonar.scalding.cassandra.CassandraSource

class ServiceProfileExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
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
    getDTOProfileInfoInTuples(profiles).write(SequenceFile(output, ProfileTuple))
            .limit(180000).write(SequenceFile(output + "_small", ProfileTuple))
}
