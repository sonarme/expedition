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

    val categories = Tsv(categoriesIn, Tuples.Behavior.CategoryAttributes).read
        .rename(('place_type, 'age, 'gender, 'income) -> ('place_type_2, 'age_2, 'gender_2, 'income_2))

    val stats = SequenceFile(statsIn, Tuples.Behavior.UserPlaceTimeMap)

    val userStats = stats.read.map('timeSegments -> 'total) {
        timeSegmentMap: Map[String, Double] => timeSegmentMap.values.sum
    }
    .discard('timeSegments)
    .leftJoinWithSmaller('userGoldenId -> 'profileId, serviceProfileDtos)
    .discard('profileId)
    .map('profileDto ->('gender, 'age, 'income)) {
        dto: ServiceProfileDTO =>
            val age = org.joda.time.Years.yearsBetween(new DateTime(dto.birthday.getTime), new DateTime()).getYears
            (dto.gender.toString, age, 50) //todo: calculate the income
    }
    .discard('profileDto)
    .crossWithTiny(categories)
    .filter('total, 'min_score, 'gender, 'gender_2, 'age, 'age_2, 'place_type, 'place_type_2, 'income, 'income_2) {
        fields: (Double, Double, String, String, Int, Int, String, String, String, String) => {
            val (totalScore, minScore, gender1, gender2, age1, age2, placeType1, placeType2, income, income2) = fields
            totalScore >= minScore && gender1.equals(gender2) && placeType1.equals(placeType2)
        }
    }
    .groupBy('userGoldenId) {
        _.toList[String]('category -> 'categories)
    }

    userStats.write(Tsv(userCategoriesOut, Tuples.Behavior.UserCategories))
}