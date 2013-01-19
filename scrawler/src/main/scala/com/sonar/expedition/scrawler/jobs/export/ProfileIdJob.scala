package com.sonar.expedition.scrawler.jobs.export

import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.twitter.scalding.{Tsv, SequenceFile, Args}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.dossier.dto.ServiceProfileLink

class ProfileIdJob(args: Args) extends DefaultJob(args) {
    val in = args("in")
    val serviceProfileFile = SequenceFile(in + "_ServiceProfile", Tuples.ProfileIdDTO)
    val profileViewFile = SequenceFile(in + "_ProfileView", Tuples.ProfileIdDTO)

    // Combine profile pipes
    val allProfiles = serviceProfileFile.read ++ profileViewFile.read
    allProfiles.map('profileId -> 'profileIdStr) {
        spl: ServiceProfileLink => spl.profileId
    }.unique('profileIdStr).write(Tsv(in + "_ids"))
}
