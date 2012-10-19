package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource

class ServiceProfileExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
    val columnFamily = args("columnFamily")
    val rpcHostArg = args("rpcHost")
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        additionalConfig = ppmap(args),
        keyspaceName = "dossier",
        columnFamilyName = columnFamily,
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    getDTOProfileInfoInTuples(profiles).write(SequenceFile(output, Tuples.Profile))
            .limit(180000).write(SequenceFile(output + "_small", Tuples.Profile))
}
