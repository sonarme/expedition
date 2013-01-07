package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.dossier.dto._
import org.scala_tools.time.Imports._
import scala.Some
import scala.Some
import com.sonar.dossier.dto.ServiceVenueDTO
import com.sonar.dossier.dto.LocationDTO
import scala.Some
import com.sonar.dossier.dto.GeodataDTO
import com.sonar.expedition.scrawler.util.Tuples
import cascading.pipe.joiner.{OuterJoin, LeftJoin}
import org.joda.time.DateMidnight
import com.sonar.dossier.dto.ServiceProfileDTO
import com.twitter.scalding.Tsv
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.jobs.behavior
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe

class UserCategoryJob(args: Args) extends Job(args) with DTOProfileInfoPipe {

    var roger = ServiceProfileDTO(ServiceType.facebook, "123")
    roger.gender = Gender.male
    roger.birthday = new DateMidnight(1981, 2, 24).toDate

    var katie = ServiceProfileDTO(ServiceType.foursquare, "234")
    katie.gender = Gender.female
    katie.birthday = new DateMidnight(1999, 1, 1).toDate

    val serviceProfileDtos = IterableSource(Seq(
        ("roger", roger),
        ("katie", katie)
    ), Tuples.ProfileIdDTO)

//    val profiles = serviceProfiles(args)

    val categoriesIn = args("categories")
    val userCategoriesOut = args("userCategories")
    val statsIn = args("userStats")

    /*
    profiles
        .filter('educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree) {
            fields: (String, String, String, String, String, String, String, String, String, String, String) => {
                val (educ, worked, city, edegree, eyear, worktitle, workdesc, impliedGender, impliedGenderProb, age, degree) = fields
                if (impliedGender != null) {
                    println(impliedGender)
                }
                worktitle != ""
            }
        }
        .write(Tsv(userCategoriesOut))
    */

    val categories = Tsv(categoriesIn, Tuples.Behavior.CategoryAttributes).read
        .rename(('placeType, 'gender, 'education) -> ('categoryPlaceType, 'categoryGender, 'categoryEducation))

    val stats = SequenceFile(statsIn, Tuples.Behavior.UserPlaceTimeMap)

    val userStats = stats.read.map('timeSegments -> 'total) {
        timeSegmentMap: Map[TimeSegment, Double] => timeSegmentMap.values.sum
    }
    .discard('timeSegments)
    .leftJoinWithSmaller('userGoldenId -> 'profileId, serviceProfileDtos)
    .discard('profileId)
    .map('profileDto ->('gender, 'age, 'income, 'education)) {
        dto: ServiceProfileDTO =>
            val age = org.joda.time.Years.yearsBetween(new DateTime(dto.birthday.getTime), new DateTime()).getYears
            (dto.gender, age, 55000, Education.unknown) //todo: calculate the income and education
    }
    .discard('profileDto)
    .crossWithTiny(categories)
    .filter('total, 'placeFrequencyThreshold, 'gender, 'categoryGender, 'age, 'ageBracket, 'placeType, 'categoryPlaceType, 'income, 'incomeBracket, 'education, 'categoryEducation) {
        fields: (Double, Double, Gender, String, Int, String, String, String, String, String, Education, String) => {
            val (totalScore, placeFrequencyThreshold, gender, categoryGender, age, ageRangeStr, placeType, categoryPlaceType, income, incomeBracket, education, categoryEducation) = fields
            val ageRange = ProfileAttributeMapping.AgeBrackets.get(ageRangeStr).getOrElse((0 to 125))
            val profileGender = ProfileAttributeMapping.Gender.get(categoryGender).getOrElse(Gender.unknown)
            val profileEducation = ProfileAttributeMapping.EducationBrackets.get(categoryEducation).getOrElse(Education.unknown)
            (
                    totalScore >= placeFrequencyThreshold &&
                    (profileGender == Gender.unknown || gender == profileGender) &&
                    placeType.equals(categoryPlaceType) && ageRange.contains(age) &&
                    ProfileAttributeMapping.IncomeBrackets.get(incomeBracket).getOrElse((0 to 100000000)).contains(income.toInt) &&
                    (education == Education.unknown || education == profileEducation)
            )
        }
    }
    .groupBy('userGoldenId) {
        _.toList[String]('profileCategory -> 'categories)
    }

    userStats.write(Tsv(userCategoriesOut, Tuples.Behavior.UserCategories))

}

object ProfileAttributeMapping {
    val Gender = Map(
        "m" -> com.sonar.dossier.dto.Gender.male,
        "f" -> com.sonar.dossier.dto.Gender.female,
        "N/A" -> com.sonar.dossier.dto.Gender.unknown
    )
    val AgeBrackets = Map(
        "0-11" -> (0 to 11),
        "12-16" -> (12 to 16),
        "17-24" -> (17 to 24),
        "25-34" -> (25 to 34),
        "35-44" -> (35 to 44),
        "45-54" -> (45 to 54),
        "55-64" -> (55 to 64),
        "65-74" -> (65 to 74),
        "75-94" -> (75 to 94),
        "Over 95" -> (95 to 125),
        "N/A" -> (0 to 125)
    )
    val IncomeBrackets = Map(
        "0-18k" -> (0 to 18000),
        "18-36k" -> (18000 to 36000),
        "36-50k" -> (36000 to 50000),
        "50-75k" -> (50000 to 75000),
        "75-100k" -> (75000 to 100000),
        "100-140k" -> (100000 to 140000),
        "140-200k" -> (140000 to 200000),
        "200-300k" -> (200000 to 300000),
        "Over 300k" -> (300000 to 100000000),
        "N/A" -> (0 to 100000000)
    )
    val EducationBrackets = Map(
        "gradeschool" -> Education.gradeschool,
        "highschool" -> Education.highschool,
        "college" -> Education.college,
        "mastersdegree" -> Education.masters,
        "doc_jd" -> Education.doctorate,
        "postdoc" -> Education.postdoc,
        "omniscient" -> Education.omniscient,
        "N/A" -> Education.unknown
    )
}