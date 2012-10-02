package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}

class DashboardQualityJob(args: Args) extends Job(args) {

    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputStatic = args("inputStatic")
    val output = args("output")
    SequenceFile(sequenceInputStatic, ('rowKey, 'columnName, 'columnValue)).read.mapTo(('rowKey, 'columnName, 'columnValue) ->('venueId, 'numberOfCheckins)) {
        in: (String, String, Double) =>
            val (rowKey, columnName, columnValue) = in
            val Array(venueId, metric) = rowKey.split("_", 2)
            if (metric == "numCheckins" && columnName == "withProfile" && columnValue >= 500) Some(venueId -> columnValue)
            else None
    }.write(Tsv(output))

}
