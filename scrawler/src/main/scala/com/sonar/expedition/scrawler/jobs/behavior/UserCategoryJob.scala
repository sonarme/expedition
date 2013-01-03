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

class UserCategoryJob(args: Args) extends Job(args) {

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

    val categoriesIn = args("categories")
    val userCategoriesOut = args("userCategories")
    val statsIn = args("userStats")

    //('ageBracket, 'gender, 'incomeBracket, 'education, 'placeType, 'placeFrequencyThreshold, 'profileCategory)
    val categories = Tsv(categoriesIn, Tuples.Behavior.CategoryAttributes).read
        .rename(('placeType, 'gender, 'education) -> ('categoryPlaceType, 'categoryGender, 'categoryEducation))

    val stats = SequenceFile(statsIn, Tuples.Behavior.UserPlaceTimeMap)

    val userStats = stats.read.map('timeSegments -> 'total) {
        timeSegmentMap: Map[String, Double] => timeSegmentMap.values.sum
    }
    .discard('timeSegments)
    .leftJoinWithSmaller('userGoldenId -> 'profileId, serviceProfileDtos)
    .discard('profileId)
    .map('profileDto ->('gender, 'age, 'income, 'education)) {
        dto: ServiceProfileDTO =>
            val age = org.joda.time.Years.yearsBetween(new DateTime(dto.birthday.getTime), new DateTime()).getYears
            (dto.gender, age, 55000, Education.college) //todo: calculate the income and education
    }
    .discard('profileDto)
    .crossWithTiny(categories)
    .filter('total, 'placeFrequencyThreshold, 'gender, 'categoryGender, 'age, 'ageBracket, 'placeType, 'categoryPlaceType, 'income, 'incomeBracket, 'education, 'categoryEducation) {
        fields: (Double, Double, Gender, String, Int, String, String, String, String, String, Education, String) => {
            val (totalScore, placeFrequencyThreshold, gender, categoryGender, age, ageRangeStr, placeType, categoryPlaceType, income, incomeBracket, education, categoryEducation) = fields
            val ageRange = ProfileAttributeMapping.AgeBrackets.get(ageRangeStr).getOrElse(Range(0,125).inclusive)
            val profileGender = ProfileAttributeMapping.Gender.get(categoryGender).getOrElse(Gender.unknown)
            (
                    totalScore >= placeFrequencyThreshold &&
                    (profileGender == Gender.unknown || gender == profileGender) &&
                    placeType.equals(categoryPlaceType) && ageRange.contains(age) &&
                    ProfileAttributeMapping.IncomeBrackets.get(incomeBracket).getOrElse(Range(0,100000000).inclusive).contains(income.toInt) &&
                    education == ProfileAttributeMapping.EducationBrackets.get(categoryEducation).getOrElse(Education.unknown)
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
        "0-11" -> Range(0,11).inclusive,
        "12-16" -> Range(12,16).inclusive,
        "17-24" -> Range(17,24).inclusive,
        "25-34" -> Range(25,34).inclusive,
        "35-44" -> Range(35,44).inclusive,
        "45-54" -> Range(45,54).inclusive,
        "55-64" -> Range(55,64).inclusive,
        "65-74" -> Range(65,74).inclusive,
        "75-94" -> Range(75,94).inclusive,
        "Over 95" -> Range(95,125).inclusive,
        "N/A" -> Range(0,125).inclusive
    )
    val IncomeBrackets = Map(
        "0-18k" -> Range(0,18000).inclusive,
        "18-36k" -> Range(18000,36000).inclusive,
        "36-50k" -> Range(36000,50000).inclusive,
        "50-75k" -> Range(50000,75000).inclusive,
        "75-100k" -> Range(75000,100000).inclusive,
        "100-140k" -> Range(100000,140000).inclusive,
        "140-200k" -> Range(140000,200000).inclusive,
        "200-300k" -> Range(200000,300000).inclusive,
        "Over 300k" -> Range(300000,100000000).inclusive,
        "N/A" -> Range(0,100000000).inclusive
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