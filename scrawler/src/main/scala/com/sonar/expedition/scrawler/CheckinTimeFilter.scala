package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import util.matching.Regex
import com.sonar.expedition.scrawler.StemAndMetaphoneEmployer
import org.apache.commons.codec.language._
import CheckinTimeFilter._
import javax.management.remote.rmi._RMIConnection_Stub
import java.text.SimpleDateFormat
import java.util

class CheckinTimeFilter {

    /* Takes in a CheckinObjects and returns a boolean indicating whether the checkin happened between 9AM and 6PM */

    def timeFilter(checkin: CheckinObjects): Boolean = {
        val timeHour = getTimeFromString(checkin.getCheckinTime)
        (timeHour < 18 && timeHour > 8)

    }

    /* Takes a string that includes the date and full time and returns just the hour as an int */

    def getTimeFromString(timeString : String): Int = {
        val timeHour = {
        timeString match {
                case ExtractTime(other,hour) => (hour.toInt)
                case _ => -1
            }
        }
            timeHour
    }

}

object CheckinTimeFilter{
    val ExtractTime: Regex = """(.*)T(\d\d).*""".r

    def parseDateTime(timestamp:String):util.Date = {
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-ddThh:mm:ss.SSSZZZZZZ")
        val parsedDate = simpleDateFormat.parse(timestamp)
        parsedDate
    }
}
