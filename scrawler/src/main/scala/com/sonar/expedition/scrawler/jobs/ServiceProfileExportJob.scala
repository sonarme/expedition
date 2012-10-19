package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.expedition.scrawler.util.Tuples

class ServiceProfileExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
    val columnFamily = args("columnFamily")
    val rpcHostArg = args("rpcHost")
    val ppmap = args.optional("ppmap") map {
        _.split(" *, *").map {
            s =>
                val Array(left, right) = s.split(':')
                ("cassandra.node.map." + left) -> right
        }.toMap
    } getOrElse (Map.empty[String, String])
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        additionalConfig = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = columnFamily,
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    getDTOProfileInfoInTuples(profiles).write(SequenceFile(output, Tuples.Profile))
            .limit(180000).write(SequenceFile(output + "_small", Tuples.Profile))
}
