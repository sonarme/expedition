package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import util.matching.Regex
import com.sonar.expedition.scrawler.pipes.AgeEducationPipe._
import com.sonar.dossier.dto.Education._
import com.sonar.dossier.dto.{ServiceType, Education}


trait AgeEducationPipe extends ScaldingImplicits {

    @deprecated
    def ageEducationPipe(serviceProfileInput: RichPipe) =
        serviceProfileInput.map(('eyear, 'edegree, 'educ) ->('age, 'degree)) {
            fields: (String, String, String) =>
                val (eyear, edegree, school) = fields
                val parsedDegree = parseDegree(edegree, school)
                val age = getAge(eyear, parsedDegree, edegree)
                (age getOrElse (-1), parsedDegree getOrElse (unknown))
        }

    def matchDegree(degree: String): Option[Education] = degree match {
        case "" => None
        case HS(str) => Some(highschool)
        case Associate(str) => Some(college)
        case MBA(str) => Some(masters)
        case Doctor(str) => Some(doctorate)
        case Masters(str) => Some(masters)
        case JD(str) => Some(doctorate)
        case Postdoc(str) => Some(postdoc)
        case Bachelors(str) => Some(college)
        case _ => Some(unknown)
    }

    def parseDegree(degreeString: String, school: String): Option[Education] = {

        val removePunc = degreeString.replaceAll( """\p{P}""", "").toLowerCase
        val list = removePunc.split("\\s+")
        val firstWord = list.headOption.getOrElse("")
        val secondWord = list.tail.headOption.getOrElse("")

        val degree = matchDegree(firstWord) orElse matchDegree(secondWord)
        val schoolList = school.replaceAll( """\p{P}""", "").toLowerCase.split("\\s+")
        val isHigh = schoolList.exists(_ == "high")

        if (isHigh)
            Some(highschool)
        else
            degree
    }

    def getAge(eYear: String, parsedDegreeOption: Option[Education], degree: String): Option[Int] =
        if (isNumeric(degree))
            Some(2012 - degree.toInt + 22)
        else if (isNumeric(eYear))
            parsedDegreeOption flatMap {
                case parsedDegree if parsedDegree == unknown => None
                case parsedDegree => Some(2012 - eYear.toInt + AgeFunction(parsedDegree))
            }
        else
            None


}

object AgeEducationPipe {
    val HS: Regex = """(hs|high)""".r
    val Associate: Regex = """a(a|s).*""".r
    val Bachelors: Regex = """(bachelor.*?|b.?.?.?.?|ab)""".r
    val MBA: Regex = """e?mba""".r
    val Masters: Regex = """(m.?.?.?|masters?|sc?m|edm)""".r
    val Doctor: Regex = """(doctor.*?|d.?.?.?.?|phd?|md|edd)""".r
    val JD: Regex = """j.?.?.?.?""".r
    val Postdoc: Regex = """post(d.*|g.*)?""".r


    val AgeFunction = Map(
        highschool -> 18,
        masters -> 25,
        doctorate -> 30,
        postdoc -> 37,
        college -> 21
    )
    final val EducationPriority = List(unknown, gradeschool, highschool, college, masters, doctorate, postdoc).zipWithIndex.toMap

}
