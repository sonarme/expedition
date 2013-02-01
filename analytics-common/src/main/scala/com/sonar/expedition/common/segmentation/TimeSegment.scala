package com.sonar.expedition.common.segmentation

case class TimeSegment(weekday: Boolean, segment: String) extends Comparable[TimeSegment] {
    def compareTo(o: TimeSegment) = Ordering[(Boolean, String)].compare((weekday, segment), (o.weekday, o.segment))

    def toIndexableString = (if (weekday) "wd" else "we") + "/" + segment
}
