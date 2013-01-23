package com.sonar.expedition.scrawler.jobs.export

import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.twitter.scalding.{IterableSource, Tsv, SequenceFile, Args}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.dossier.dto.{ServiceProfileDTO, ServiceType, ServiceProfileLink}
import cascading.pipe.Pipe

class ProfileIdJob(args: Args) extends DefaultJob(args) {
    val in = args("in")
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)
    val serviceProfileFile: Pipe =
        if (test)

            IterableSource(
                (for (serviceType <- ServiceType.values().toIterable;
                      name <- 1 to 100) yield
                    (ServiceProfileLink(serviceType, name.toString), ServiceProfileDTO(serviceType, name.toString), serviceType))

                , Tuples.ProfileIdDTO).read
        else SequenceFile(in + "_ServiceProfile", Tuples.ProfileIdDTO).read
    val profileViewFile: Pipe =
        if (test)
            IterableSource(
                (for (serviceType <- ServiceType.values().toIterable;
                      name <- 50 to 150) yield
                    (ServiceProfileLink(serviceType, name.toString), ServiceProfileDTO(serviceType, name.toString), serviceType))
                , Tuples.ProfileIdDTO).read
        else SequenceFile(in + "_ProfileView", Tuples.ProfileIdDTO).read

    // Combine profile pipes
    val allProfiles = (serviceProfileFile ++ profileViewFile).mapTo(('profileId, 'serviceType) ->('profileId, 'serviceType)) {
        in: (ServiceProfileLink, ServiceType) => (in._1, in._2.name())
    }

    val numServiceType =
        allProfiles.unique('profileId, 'serviceType).groupBy('serviceType) {
            _.size
        }.map('serviceType -> 'statName) {
            serviceType: String => "num_" + serviceType
        }.project('statName, 'size)
    numServiceType.write(Tsv(in + "_stats", ('statName, 'size)))

}
