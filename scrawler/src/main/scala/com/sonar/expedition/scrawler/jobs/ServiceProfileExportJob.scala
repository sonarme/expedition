package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.scalding.cassandra.CassandraSource

class ServiceProfileExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
    val rpcHostArg = args("rpcHost")
    val output = args("output")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        keyspaceName = "dossier",
        columnFamilyName = "ProfileView",
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameField = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    getDTOProfileInfoInTuples(profiles).write(SequenceFile(output, ProfileTuple))
            .limit(180000).write(SequenceFile(output + "_small", ProfileTuple))
}
