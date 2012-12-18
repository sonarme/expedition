package com.sonar.expedition.scrawler.checkins

import java.util.Date
import com.sonar.expedition.scrawler.util.TimezoneLookup
import org.joda.time.LocalDateTime
import org.joda.time.DateTimeConstants._
import de.fuberlin.wiwiss.silk.plugins.metric.GeographicDistanceMetric
import cascading.pipe.Pipe
import com.sonar.expedition.scrawler.pipes.ScaldingImplicits
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{SimpleDistanceMeasure, DistanceMeasure}
import cascading.tuple.Fields

trait CheckinInference extends ScaldingImplicits {
    val WeekDays = Set(MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY)

    def localDateTime(lat: Double, lng: Double, checkinTime: Date) = {
        val localTz = TimezoneLookup.getClosestTimeZone(lat, lng)
        new LocalDateTime(checkinTime, localTz)

    }


    def addIndex(pipe: Pipe, measure: SimpleDistanceMeasure, threshold: Int) = pipe.flatMap('location ->('locationStr, 'indexEl)) {
        location: (Double, Double) =>
            val (lat, lng) = location
            val locationStr = lat + " " + lng
            measure.indexValue(locationStr, threshold).flatten map {
                idx => (locationStr, idx)
            }
    }

    def matchGeo(fieldDef: Fields, smaller: Pipe, larger: Pipe, measure: SimpleDistanceMeasure = GeographicDistanceMetric("m"), threshold: Int = 50, top: Int = 3) =
        addIndex(smaller, measure, threshold).rename('locationStr -> 'locationSmaller)
                .joinWithLarger('indexEl -> 'indexEl, addIndex(larger, measure, threshold).rename('locationStr -> 'locationLarger)).flatMap(('locationSmaller, 'locationLarger) -> 'distance) {
            in: (String, String) =>
                val (locationSmaller, locationLarger) = in
                val distance = measure.evaluate(locationSmaller, locationLarger)
                if (distance > threshold) None else Some(-distance)
        }.groupBy('indexEl) {
            _.sortedReverseTake[Double]('distance -> 'topEls, top)
        }.discard('indexEl).flatten[Double]('topEls -> 'topEls)

    def isWeekDay(ldt: LocalDateTime) = WeekDays(ldt.getDayOfWeek)

}
