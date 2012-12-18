package com.sonar.expedition.scrawler.checkins

import java.util.Date
import com.sonar.expedition.scrawler.util.TimezoneLookup
import org.joda.time.LocalDateTime
import org.joda.time.DateTimeConstants._
import de.fuberlin.wiwiss.silk.plugins.metric.GeographicDistanceMetric
import cascading.pipe.Pipe
import com.sonar.expedition.scrawler.pipes.ScaldingImplicits
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{SimpleDistanceMeasure, DistanceMeasure}
import cascading.tuple.{Tuple, Fields}
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

    def matchGeo(smaller: (Pipe, Fields, Fields), larger: (Pipe, Fields, Fields), distance: Fields, metaFields: Fields, measure: SimpleDistanceMeasure = GeographicDistanceMetric("m"), threshold: Int = 50, top: Int = 3) = {
        val (smallerPipe, smallerBlockId, smallerLocation) = smaller
        val (largerPipe, largerBlockId, largerLocation) = larger
        val blockIds = Fields.merge(smallerBlockId, largerBlockId)
        val groupFields = 'indexEl append blockIds
        val distanceAndMetaFields = distance append metaFields subtract blockIds
        addIndex(smallerPipe, smallerLocation, measure, threshold)
                .joinWithLarger(('indexEl append smallerBlockId) -> ('indexEl append largerBlockId), addIndex(largerPipe, largerLocation, measure, threshold))
                .flatMap((smallerLocation, largerLocation) -> distance) {
            in: (GeodataDTO, GeodataDTO) =>
                val (locationSmaller, locationLarger) = in
                val distance = measure.evaluate(locationSmaller.canonicalId, locationLarger.canonicalId)
                if (distance > threshold) None else Some(distance)
        }.groupBy(groupFields) {
            _.sortWithTake[Tuple](distanceAndMetaFields -> 'topEls, top) {
                (left: Tuple, right: Tuple) =>
                    left.getDouble(0) > right.getDouble(0)
            }
        }.discard('indexEl).flatten[Tuple]('topEls -> distanceAndMetaFields)
    }

    def isWeekDay(ldt: LocalDateTime) = WeekDays(ldt.getDayOfWeek)

}
