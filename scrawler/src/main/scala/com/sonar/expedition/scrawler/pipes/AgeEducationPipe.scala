package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import util.matching.Regex
import com.sonar.expedition.scrawler.pipes.AgeEducationPipe._


trait AgeEducationPipe extends ScaldingImplicits {

    def ageEducationPipe(serviceProfileInput: RichPipe) =
        serviceProfileInput.map(('eyear, 'edegree, 'educ) ->('age, 'degree)) {
            fields: (String, String, String) =>
                val (eyear, edegree, school) = fields
                val parsedDegree = parseDegree(edegree, school)
                val age = getAge(eyear, parsedDegree, edegree)
                (age, parsedDegree)
        }

    def parseDegree(orig: String, school: String): String = {

        val removePunc = orig.replaceAll( """\p{P}""", "").toLowerCase
        val list = removePunc.split("\\s+")
        val firstWord = list.headOption.getOrElse("")
        val secondWord = list.tail.headOption.getOrElse("")
        val seconddegree = secondWord match {
            case "" => "NA"
            case HS(str) => "H"
            case Associate(str) => "A"
            case MBA() => "MBA"
            case Doctor(str) => "D"
            case Masters(str) => "M"
            case JD() => "J"
            case Postdoc(str) => "P"
            case Bachelors(str) => "B"
            case _ => "O"
        }
        val degree = firstWord match {
            case "" => "NA"
            case HS(str) => "H"
            case Associate(str) => "A"
            case MBA() => "MBA"
            case Doctor(str) => "D"
            case Masters(str) => "M"
            case JD() => "J"
            case Postdoc(str) => "P"
            case Bachelors(str) => "B"
            case _ => seconddegree
        }

        val schoolList = school.replaceAll( """\p{P}""", "").toLowerCase.split("\\s+")
        val isHigh = schoolList.exists(_ == "high")

        if (isHigh)
            "H"
        else
            degree
    }

    def getAge(eYear: String, parsedDegree: String, degree: String): Int = {


        if (isNumeric(degree) && degree != "")
            2012 - degree.toInt + 22
        else if (!isNumeric(eYear) || eYear == "")
            -1
        else
            2012 - eYear.toInt + AgeFunction(parsedDegree)

    }

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
        "H" -> 18,
        "A" -> 20,
        "MBA" -> 28,
        "D" -> 32,
        "M" -> 24,
        "J" -> 27,
        "P" -> 37,
        "B" -> 22,
        "O" -> 22,
        "NA" -> 22
    )
}
