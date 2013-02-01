package com.sonar.expedition.common.segmentation

import org.joda.time.LocalDateTime
import java.util.Date
import org.joda.time.DateTimeConstants._
import com.sonar.expedition.common.util.TimezoneLookup

trait TimeSegmentation {
    val WeekDays = Set(MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY)

    def isWeekDay(ldt: LocalDateTime) = WeekDays(ldt.getDayOfWeek)

    def localDateTime(lat: Double, lng: Double, checkinTime: Date) = {
        val localTz = TimezoneLookup.getClosestTimeZone(lat, lng)
        new LocalDateTime(checkinTime, localTz)
    }

    def hourSegment(lat: Double, lng: Double, checkinTime: Date) = {
        val ldt = localDateTime(lat, lng, checkinTime)
        TimeSegment(isWeekDay(ldt), ldt.getHourOfDay.toString)
    }
}
