package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.scalding.cassandra.CassandraSource

class ServiceProfileExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
    val rpcHostArg = args("rpcHost")
    val ppmapStr = args.getOrElse("ppmap", "")
    val ppmap = ppmapStr.split(" *, *").map {
        s =>
            val Array(left, right) = s.split(':')
            ("cassandra.node.map." + left) -> right
    }.toMap
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        additionalConfig = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "ProfileView",
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    getDTOProfileInfoInTuples(profiles).write(SequenceFile(output))
            .limit(180000).write(SequenceFile(output + "_small"))
}
