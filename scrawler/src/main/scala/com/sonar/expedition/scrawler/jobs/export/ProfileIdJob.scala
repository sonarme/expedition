package com.sonar.expedition.scrawler.jobs.export

import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.twitter.scalding.{Tsv, SequenceFile, Args}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.dossier.dto.{ServiceType, ServiceProfileLink}

class ProfileIdJob(args: Args) extends DefaultJob(args) {
    val in = args("in")
    val serviceProfileFile = SequenceFile(in + "_ServiceProfile", Tuples.ProfileIdDTO)
    val profileViewFile = SequenceFile(in + "_ProfileView", Tuples.ProfileIdDTO)

    // Combine profile pipes
    val allProfiles = (serviceProfileFile.read ++ profileViewFile.read).map('profileId -> 'profileId) {
        spl: ServiceProfileLink => spl.profileId
    }
    val numServiceType =
        allProfiles.discard('profileDto).unique('profileId, 'serviceType).groupBy('serviceType) {
            _.size.reducers(1)
        }.map('serviceType -> 'statName) {
            serviceType: ServiceType => "num_" + serviceType.name()
        }.project('statName, 'size)
    numServiceType.write(Tsv(in + "_stats2", ('statName, 'size)))

}
