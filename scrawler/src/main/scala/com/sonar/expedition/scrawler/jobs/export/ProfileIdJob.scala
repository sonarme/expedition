package com.sonar.expedition.scrawler.jobs.export

import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.twitter.scalding.{Tsv, SequenceFile, Args}
import com.sonar.expedition.scrawler.util.Tuples

class ProfileIdJob(args: Args) extends DefaultJob(args) {
    val in = args("in")
    val serviceProfileFile = SequenceFile(in + "_ServiceProfile", Tuples.ProfileIdDTO)
    val profileViewFile = SequenceFile(in + "_ProfileView", Tuples.ProfileIdDTO)

    // Combine profile pipes
    val allProfiles = serviceProfileFile.read ++ profileViewFile.read
    allProfiles.unique('profileId).write(Tsv(in + "_ids"))
}
