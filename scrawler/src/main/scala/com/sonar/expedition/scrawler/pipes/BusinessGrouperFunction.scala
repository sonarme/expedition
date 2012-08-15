package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Job, Args}
import java.util.Calendar
import util.matching.Regex
import com.sonar.expedition.scrawler.util.CommonFunctions._

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
                .map('loc -> 'venueKey) {
            loc: String => loc
        }
        //                .project('keyid, 'serType, 'impliedGender, 'age, 'degree, 'venueKey, 'hourChunk, 'dayChunk)
    }

    def timeSeries(combinedInput: RichPipe): RichPipe = {
        combinedInput.groupBy('venueKey, 'hourChunk, 'serType) {
            _.size
        }
    }

    def byAge(combinedInput: RichPipe): RichPipe = {
        combinedInput
                .map('age -> 'ageBracket) {
            age: Int => {
                if (age < 0)
                    "unknown"
                else if (age < 18)
                    "<18"
                else if (age < 25)
                    "18-24"
                else if (age < 35)
                    "25-34"
                else if (age < 45)
                    "35-44"
                else if (age < 55)
                    "45-54"
                else if (age < 65)
                    "55-64"
                else
                    "65+"
            }
        }
                .filter('ageBracket) {
            age: String => !age.equals("unknown")
        }
                .groupBy('ageBracket, 'venueKey) {
            // .groupBy('ageBracket, 'venueKey, 'hourChunk) {
            _.size
        }
    }

    def byGender(combinedInput: RichPipe): RichPipe = {
        combinedInput
                .filter('impliedGender) {
            gend: Gender => !(gend == Gender.unknown)
        }
                // .groupBy('impliedGender, 'venueKey, 'hourChunk) {
                .groupBy('impliedGender, 'venueKey) {
            _.size
        }
    }

    def byDegree(combinedInput: RichPipe): RichPipe = {
        combinedInput
                .map('degree -> 'degreeCat) {
            degree: String => {
                degree match {
                    case College(str) => "College"
                    case NoCollege(str) => "No College"
                    case Grad(str) => "Grad School"
                    case _ => "unknown"
                }
            }
        }
                .filter('degreeCat) {
            degree: String => !degree.equals("unknown")
        }
                // .groupBy('degreeCat, 'venueKey, 'hourChunk) {
                .groupBy('degreeCat, 'venueKey) {
            _.size
        }
    }

    def byIncome(combinedInput: RichPipe): RichPipe = {
        combinedInput
                .filter('worktitle) {
            worktitle: String => !isNullOrEmpty(worktitle)
        }
                .map('income -> 'incomeBracket) {
            income: String => {
                val incomeInt = income.replaceAll("\\D", "").toInt
                if (incomeInt < 50000)
                    "$0-50k"
                else if (incomeInt < 100000)
                    "$50-100k"
                else if (incomeInt < 150000)
                    "$100-150k"
                else
                    "$150k+"
            }
        }
                .groupBy('incomeBracket, 'venueKey) {
            _.size
        }

    }


}
