package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Job, Args}
import java.util.Calendar
import util.matching.Regex
import BusinessGrouperFunction._

class BusinessGrouperFunction(args: Args) extends Job(args) {

    def combineCheckinsProfiles(checkinInput: RichPipe, serviceProfileInput: RichPipe): RichPipe = {

        //('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'age, 'degree, 'impliedGender)
        //('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'dayOfWeek, 'hour)
        checkinInput.joinWithSmaller('keyid -> 'key, serviceProfileInput)
                .map('chknTime ->('hourChunk, 'dayChunk)) {
            checkinTime: String => {
                val timeFilter = Calendar.getInstance()
                val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                timeFilter.setTime(checkinDate)
                val hour = (timeFilter.getTimeInMillis / 3600000) // 1000 * 60 * 60  = for hour chunks
                val day = (timeFilter.getTimeInMillis / 86400000) // 1000 * 60 * 60 * 24 = for 24 hour chunks
                (hour, day)
            }
        }
                .project('keyid, 'impliedGender, 'age, 'degree, 'loc, 'hourChunk, 'dayChunk)
    }

    def byAge(combinedInput: RichPipe): RichPipe = {
        combinedInput
                .map('age -> 'ageBracket) {
            age: Int => {
                if (age < 0)
                    "unknown"
                else if (age < 18)
                    "< 18"
                else if (age < 25)
                    "18-24"
                else if (age < 31)
                    "25-30"
                else if (age < 41)
                    "31 - 40"
                else if (age < 51)
                    "41 - 50"
                else
                    "> 50"
            }
        }
                .groupBy('ageBracket, 'loc) {
            // .groupBy('ageBracket, 'loc, 'hourChunk) {
            _.size
        }
    }

    def byGender(combinedInput: RichPipe): RichPipe = {
        combinedInput
                // .groupBy('impliedGender, 'loc, 'hourChunk) {
                .groupBy('impliedGender, 'loc) {
            _.size
        }
    }

    def byDegree(combinedInput: RichPipe): RichPipe = {
        combinedInput
                .map('degree -> 'degreeCat) {
            degree: String => {
                degree match {
                    case College(str) => "College"
                    case None(str) => "No College"
                    case Grad(str) => "Grad School"
                    case _ => "unknown"
                }
            }
        }
                // .groupBy('degreeCat, 'loc, 'hourChunk) {
                .groupBy('degreeCat, 'loc) {
            _.size
        }
    }

}

object BusinessGrouperFunction {
    val College: Regex = """(A|B|O)""".r
    val None: Regex = """(H)""".r
    val Grad: Regex = """(D|M|MBA|J|P)""".r
}
