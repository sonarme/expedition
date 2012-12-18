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
import com.sonar.dossier.dto.GeodataDTO

trait CheckinInference extends ScaldingImplicits {
    val WeekDays = Set(MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY)

    def localDateTime(lat: Double, lng: Double, checkinTime: Date) = {
        val localTz = TimezoneLookup.getClosestTimeZone(lat, lng)
        new LocalDateTime(checkinTime, localTz)

    }


    def addIndex(pipe: Pipe, location: Fields, measure: SimpleDistanceMeasure, threshold: Int) = pipe.flatMap(location -> 'indexEl) {
        location: GeodataDTO =>
            measure.indexValue(location.canonicalId, threshold).flatten
    }

    def matchGeo(smaller: (Pipe, Fields, Fields, Fields), larger: (Pipe, Fields, Fields, Fields), distance: Fields, measure: SimpleDistanceMeasure = GeographicDistanceMetric("m"), threshold: Int = 50, top: Int = 3) = {
        val (smallerPipe, smallerId, smallerBlockId, smallerLocation) = smaller
        val (largerPipe, largerId, largerBlockId, largerLocation) = larger
        addIndex(smallerPipe, smallerLocation, measure, threshold)
                .joinWithLarger((smallerBlockId, 'indexEl) ->(largerBlockId, 'indexEl), addIndex(largerPipe, largerLocation, measure, threshold))
                .flatMap((smallerLocation, largerLocation) -> distance) {
            in: (GeodataDTO, GeodataDTO) =>
                val (locationSmaller, locationLarger) = in
                val distance = measure.evaluate(locationSmaller.canonicalId, locationLarger.canonicalId)
                if (distance > threshold) None else Some(distance)
        }.groupBy(smallerBlockId, largerBlockId, 'indexEl) {
            _.sortWithTake[(String, String, Double)]((smallerId, largerId, distance) -> 'topEls, top) {
                (left: (String, String, Double), right: (String, String, Double)) =>
                    left._3 > right._3
            }
        }.discard('indexEl).flatten[(String, String, Double)]('topEls ->(smallerId, largerId, distance))
    }

    def isWeekDay(ldt: LocalDateTime) = WeekDays(ldt.getDayOfWeek)

}
