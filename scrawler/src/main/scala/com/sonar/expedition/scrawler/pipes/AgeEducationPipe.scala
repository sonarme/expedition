package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Job, Args}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import util.matching.Regex
import com.sonar.expedition.scrawler.pipes.AgeEducationPipe._

class AgeEducationPipe(args: Args) extends Job(args){

    def ageEducationPipe(serviceProfileInput: RichPipe): RichPipe = {

        // ('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid (twalias if not total data), 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)

        val age = serviceProfileInput.map(('eyear, 'edegree) -> ('age, 'degree)){
            fields: (String, String) => {
                val (eyear, edegree) = fields
                val parsedDegree = parseDegree(edegree)
                val age = getAge(eyear, parsedDegree)
                (age, parsedDegree)

            }
        }

        age

    }

    def parseDegree(orig: String): String = {

        val removePunc = orig.replaceAll( """\p{P}""", "").toLowerCase
        val list = removePunc.split("\\s+")
        val firstWord = list.headOption.getOrElse("")
        val secondWord = list.tail.headOption.getOrElse("")
        val seconddegree = secondWord match {
            case "" => "O"
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
            case "" => "O"
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
        degree
    }

    def getAge(eYear: String, parsedDegree: String): Int = {
        val agefunction = Map[String, Int](
            "H" -> 18,
            "A" -> 20,
            "MBA" -> 28,
            "D" -> 32,
            "M" -> 24,
            "J" -> 27,
            "P" -> 37,
            "B" -> 22,
            "O" -> 22
        )


        if (!isNumeric(eYear) || eYear.equals(""))
            -1
        else
            2012 - eYear.toInt + agefunction.get(parsedDegree).get

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

}
