package com.sonar.expedition.scrawler.pipes

import com.sonar.expedition.scrawler.objs.CheckinObjects
import CheckinTimeFilter._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.util.matching.Regex

class CheckinTimeFilter {

    /* Takes in a CheckinObjects and returns a boolean indicating whether the checkin happened between 9AM and 6PM */

    def timeFilter(checkin: CheckinObjects): Boolean = {
        val timeHour = getTimeFromString(checkin.getCheckinTime)
        (timeHour < 18 && timeHour > 8)

    }

    /* Takes a string that includes the date and full time and returns just the hour as an int */

    def getTimeFromString(timeString: String): Int = {
        val timeHour = {
            timeString match {
                case ExtractTime(other, hour) => (hour.toInt)
                case _ => -1
            }
        }
        timeHour
    }

}

object CheckinTimeFilter {
    val ExtractTime: Regex = """(.*)T(\d\d).*""".r

    val TimezoneColon = """([\d\-:T]+\.[\d]+[\+\-][\d]+):([\d]+)""".r

    def parseDateTime(timestamp: String): Date = {
        val parsedTimestamp = removeTrailingTimezoneColon(timestamp)
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ")
        val parsedDate = try {
            simpleDateFormat.parse(parsedTimestamp)
        }
        catch {
            case _ => {
                val defaultTime = Calendar.getInstance()
                defaultTime.set(Calendar.YEAR, 1927)
                defaultTime.getTime
            }
        }
        parsedDate
    }

    /**
     * There seems to be a non-standard colon that gets embedded into the timestamp, so we need to remove this
     * so that we can parse using simpleDateFormat
     * @param timestamp
     */
    def removeTrailingTimezoneColon(timestamp: String):String = {
        TimezoneColon findFirstIn timestamp match {
            case Some(TimezoneColon(dateTime, remainingTimeZone)) => dateTime + remainingTimeZone
            case None => timestamp
        }
    }
}
