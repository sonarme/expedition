package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.expedition.scrawler.util.Tuples

class ProfileCountJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe {
    (SequenceFile(args("input1"), Tuples.Profile).read.project('profileId)
            ++ SequenceFile(args("input2"), Tuples.Profile).read.project('profileId))
            .unique('profileId)
            .write(Tsv(args("output")))
}
