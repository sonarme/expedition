package com.sonar.expedition.scrawler.jobs.behavior

case class TimeSegment(weekday: Boolean, segment: String) extends Comparable[TimeSegment] {
    def compareTo(o: TimeSegment) = Ordering[(Boolean, String)].compare((weekday, segment), (o.weekday, o.segment))

    def toIndexableString = weekday.toString + "_" + segment
}
