package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.dossier.dto._
import org.scala_tools.time.Imports._
import com.sonar.expedition.scrawler.util.Tuples
import org.joda.time.DateMidnight
import com.sonar.dossier.dto.ServiceProfileDTO
import com.twitter.scalding.Tsv
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.pipes.{AgeEducationPipe, DTOProfileInfoPipe}
import ProfileAttributeMapping._
import com.sonar.expedition.scrawler.jobs.{DefaultJob, Csv}
import collection.JavaConversions._
import TimeSegment

class UserCategoryJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe with AgeEducationPipe {

    val test = args.optional("test").map(_.toBoolean).getOrElse(false)

    val serviceProfileDtos = if (test) IterableSource(Seq(
        ("roger", {
            val roger = ServiceProfileDTO(ServiceType.facebook, "123")
            roger.gender = Gender.male
            roger.birthday = new DateMidnight(1981, 2, 24).toDate
            roger
        }),
        ("katie", {
            val katie = ServiceProfileDTO(ServiceType.foursquare, "234")
            katie.gender = Gender.female
            katie.birthday = new DateMidnight(1999, 1, 1).toDate
            katie
        })
    ), Tuples.ProfileIdDTO)
    else SequenceFile(args("serviceProfilesIn"), Tuples.ProfileIdDTO)

    val categoriesIn = args("categoriesIn")
    val userCategoriesOut = args("userCategoriesOut")
    val statsIn = args("userStatsIn")

    val categories = Csv(categoriesIn, Tuples.Behavior.CategoryAttributes).read
            .rename(('placeType, 'gender, 'education) ->('categoryPlaceType, 'categoryGender, 'categoryEducation))

    val stats = SequenceFile(statsIn, Tuples.Behavior.UserPlaceTimeMap)

    val userStats = stats.read.map('timeSegments -> 'total) {
        timeSegmentMap: Map[TimeSegment, Double] => timeSegmentMap.values.sum
    }
            .discard('timeSegments)
            .leftJoinWithSmaller('userGoldenId -> 'profileId, serviceProfileDtos)
            .discard('profileId)
            .map('profileDto ->('gender, 'age, 'income, 'education)) {
        dto: ServiceProfileDTO =>
            val educations = for (education <- dto.education) yield {
                val parsedDegree = parseDegree(education.degree, education.schoolName)
                val age = getAge(education.year, parsedDegree, education.degree)
                (parsedDegree, age)
            }

            val highestEducation = if (educations.isEmpty) Education.unknown else educations.map(_._1).flatten.maxBy(AgeEducationPipe.EducationPriority)
            val age = if (dto.birthday == null) {
                if (educations.isEmpty) None
                else educations.map(_._2).flatten.max
            }
            else Some(org.joda.time.Years.yearsBetween(new DateTime(dto.birthday), DateTime.now).getYears)

            (dto.gender, age, 55000, highestEducation) //todo: calculate the income and education
    }
            .discard('profileDto)
            .joinWithTiny('placeType -> 'categoryPlaceType, categories)
            .filter('total, 'placeFrequencyThreshold, 'gender, 'categoryGender, 'age, 'ageBracket, 'income, 'incomeBracket, 'education, 'categoryEducation) {
        fields: (Double, Double, Gender, String, Int, String, Int, String, Education, String) => {
            val (totalScore, placeFrequencyThreshold, gender, categoryGender, age, ageRangeStr, income, incomeBracket, education, categoryEducation) = fields
            lazy val ageRange = AgeBrackets(ageRangeStr)
            lazy val profileGender = GenderBrackets(categoryGender)
            lazy val profileEducation = EducationBrackets(categoryEducation)
            lazy val incomeRange = IncomeBrackets(incomeBracket)
            totalScore >= placeFrequencyThreshold &&
                    (profileGender == Gender.unknown || gender == profileGender) &&
                    (education == Education.unknown || education == profileEducation) &&
                    ageRange.contains(age) &&
                    incomeRange.contains(income)

        }
    }
            .groupBy('userGoldenId) {
        _.toList[String]('profileCategory -> 'categories)
    }

    userStats.write(Tsv(userCategoriesOut, Tuples.Behavior.UserCategories))

}

object ProfileAttributeMapping {
    val GenderBrackets = Map(
        "m" -> Gender.male,
        "f" -> Gender.female
    ) withDefaultValue (Gender.unknown)
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
        "Over 95" -> (95 to Int.MaxValue)
    ) withDefaultValue (0 to Int.MaxValue - 1)
    val IncomeBrackets = Map(
        "0-18k" -> (0 to 18000),
        "18-36k" -> (18000 to 36000),
        "36-50k" -> (36000 to 50000),
        "50-75k" -> (50000 to 75000),
        "75-100k" -> (75000 to 100000),
        "100-140k" -> (100000 to 140000),
        "140-200k" -> (140000 to 200000),
        "200-300k" -> (200000 to 300000),
        "Over 300k" -> (300000 to Int.MaxValue)
    ) withDefaultValue (0 to Int.MaxValue - 1)
    val EducationBrackets = Map(
        "gradeschool" -> Education.gradeschool,
        "highschool" -> Education.highschool,
        "college" -> Education.college,
        "mastersdegree" -> Education.masters,
        "doc_jd" -> Education.doctorate,
        "postdoc" -> Education.postdoc,
        "omniscient" -> Education.omniscient
    ) withDefaultValue (Education.unknown)
}
