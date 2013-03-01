package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.dossier.dto._
import org.joda.time.DateMidnight
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.pipes.AgeEducationPipe
import com.sonar.dossier.dto.ServiceProfileDTO
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.twitter.scalding.IterableSource
import collection.JavaConversions._
import com.sonar.expedition.scrawler.Encryption._

class SonarFeatureJob(args: Args) extends DefaultJob(args) with AgeEducationPipe {
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)
    val profiles = if (test) IterableSource(Seq(
        ("roger", {
            val roger = ServiceProfileDTO(ServiceType.facebook, "123")
            roger.gender = Gender.male
            roger.birthday = new DateMidnight(1981, 2, 24).toDate
            roger
        }, ServiceType.sonar),
        ("katie", {
            val katie = ServiceProfileDTO(ServiceType.foursquare, "234")
            katie.gender = Gender.female
            katie.birthday = new DateMidnight(1999, 1, 1).toDate
            katie
        }, ServiceType.sonar),
        ("ben", {
            val ben = ServiceProfileDTO(ServiceType.sonar, "2345")
            ben.gender = Gender.male
            ben.education = Seq(UserEducation(year = "2004", degree = "BS"), UserEducation(year = "2005", degree = "MSc"))
            ben
        }, ServiceType.sonar)
    ), Tuples.ProfileIdDTO)
    else SequenceFile(args("profilesIn"), Tuples.ProfileIdDTO)
    profiles.read.flatMapTo(('profileId, 'profileDto, 'serviceType) ->('profileId, 'gender, 'age)) {
        in: (String, ServiceProfileDTO, ServiceType) =>
            val (profileId, dto, serviceType) = in
            if (profileId.startsWith("sonar")) {
                val (age, _) = getAgeAndEducation(dto, education = false)
                Some(dto.serviceType.name() + "-" + encrypt(dto.userId), dto.gender.name(), age.getOrElse(null))
            } else None
    }.write(Tsv(args("featuresOut")))

}
