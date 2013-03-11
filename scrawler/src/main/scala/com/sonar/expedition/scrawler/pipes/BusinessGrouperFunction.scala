package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import java.util.{Date, Calendar}


trait BusinessGrouperFunction extends ScaldingImplicits {
    def parseIncome(incomeStr: String) =
        if (incomeStr == null) -1
        else {
            val clean = incomeStr.replaceAll("\\D", "")
            if (clean.isEmpty) -1 else clean.toInt
        }

    def chunkTime(checkinInput: RichPipe) =
        checkinInput.map('chknTime ->('hourChunk, 'dayChunk)) {
            checkinTime: Date =>
                val timeFilter = Calendar.getInstance()
                val checkinDate = checkinTime
                timeFilter.setTime(checkinDate)
                val hour = (timeFilter.getTimeInMillis / 3600000) // 1000 * 60 * 60  = for hour chunks
                val day = (timeFilter.getTimeInMillis / 86400000) // 1000 * 60 * 60 * 24 = for 24 hour chunks
                (hour, day)
        }

    def combineCheckinsProfiles(checkinInput: RichPipe, serviceProfileInput: RichPipe) =
        checkinInput
                .joinWithSmaller('keyid -> 'key, serviceProfileInput)
                .map('goldenId -> 'venueKey) {
            goldenId: String => goldenId
        }

    def timeSeries(combinedInput: RichPipe) =
        combinedInput.groupBy('venueKey, 'hourChunk, 'serType) {
            _.size
        }

    def groupByAge(combinedInput: RichPipe) =
        combinedInput
                .flatMap('age -> 'ageBracket) {
            ageOpt: Option[Int] =>
                ageOpt flatMap {
                    age =>
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
                }
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

    import com.sonar.dossier.dto.Education

    def groupByDegree(combinedInput: RichPipe) =
        combinedInput.map('degree -> 'degreeCat) {
            degree: Education =>
                degree match {
                    case Education.college => "College"
                    case Education.gradeschool | Education.highschool => "NoCollege"
                    case Education.doctorate | Education.masters | Education.postdoc => "GradSchool"
                    case _ => "unknown"
                }
        }.filter('degreeCat) {
            degree: String => degree != "unknown"
        }.groupBy('degreeCat, 'venueKey) {
            _.size
        }


    def groupByIncome(combinedInput: RichPipe) =
        combinedInput
                .flatMap('income -> 'incomeBracket) {
            incomeStr: String => {
                val income = parseIncome(incomeStr)
                if (income >= 0) {
                    Some(
                        if (income < 50000)
                            "$0-50k"
                        else if (income < 100000)
                            "$50-100k"
                        else if (income < 150000)
                            "$100-150k"
                        else
                            "$150k+")
                } else None

            }
        }.groupBy('incomeBracket, 'venueKey) {
            _.size
        }


}
