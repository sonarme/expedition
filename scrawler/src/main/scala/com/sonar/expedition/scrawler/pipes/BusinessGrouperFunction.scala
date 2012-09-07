package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import java.util.{Date, Calendar}
import util.matching.Regex
import com.sonar.expedition.scrawler.util.CommonFunctions._


trait BusinessGrouperFunction extends ScaldingImplicits {

    def combineCheckinsProfiles(checkinInput: RichPipe, serviceProfileInput: RichPipe) =
        checkinInput.joinWithSmaller('keyid -> 'key, serviceProfileInput)
                .map('chknTime ->('hourChunk, 'dayChunk)) {
            checkinTime: Date =>
                val timeFilter = Calendar.getInstance()
                val checkinDate = checkinTime
                timeFilter.setTime(checkinDate)
                val hour = (timeFilter.getTimeInMillis / 3600000) // 1000 * 60 * 60  = for hour chunks
                val day = (timeFilter.getTimeInMillis / 86400000) // 1000 * 60 * 60 * 24 = for 24 hour chunks
                (hour, day)
        }.map('goldenId -> 'venueKey) {
            goldenId: String => goldenId
        }

    def timeSeries(combinedInput: RichPipe) =
        combinedInput.groupBy('venueKey, 'hourChunk, 'serType) {
            _.size
        }

    def groupByAge(combinedInput: RichPipe) =
        combinedInput
                .flatMap('age -> 'ageBracket) {
            age: Int =>
                if (age < 0)
                    None
                else Some(if (age < 18)
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
                    "65+")
        }.groupBy('ageBracket, 'venueKey) {
            _.size
        }

    def groupByGender(combinedInput: RichPipe) =
        combinedInput
                .filter('impliedGender) {
            gender: String => gender != "unknown"
        }.groupBy('impliedGender, 'venueKey) {
            _.size
        }

    def groupByDegree(combinedInput: RichPipe) =
        combinedInput
                .map('degree -> 'degreeCat) {
            degree: String =>
                degree match {
                    case College(str) => "College"
                    case NoCollege(str) => "No College"
                    case Grad(str) => "Grad School"
                    case _ => "unknown"
                }
        }.filter('degreeCat) {
            degree: String => degree != "unknown"
        }.groupBy('degreeCat, 'venueKey) {
            _.size
        }


    def groupByIncome(combinedInput: RichPipe) =
        combinedInput
                .filter('worktitle) {
            worktitle: String => !isNullOrEmpty(worktitle)
        }.map('income -> 'incomeBracket) {
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
        }.groupBy('incomeBracket, 'venueKey) {
            _.size
        }


}
