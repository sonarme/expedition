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
import com.sonar.dossier.dto.GeodataDTO
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings

trait CheckinInference extends ScaldingImplicits {
    val WeekDays = Set(MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY)

    def localDateTime(lat: Double, lng: Double, checkinTime: Date) = {
        val localTz = TimezoneLookup.getClosestTimeZone(lat, lng)
        new LocalDateTime(checkinTime, localTz)

    }


    def addIndex(pipe: Pipe, locationField: Fields, measure: SimpleDistanceMeasure, threshold: Int) = pipe.flatMap(locationField -> 'indexEl) {
        location: GeodataDTO =>
        /*
        // complicated because it creates dupes
        val latLng = location.canonicalLatLng
        val indices = measure.indexValue(latLng, threshold).flatten
        println("indices:" + indices)
        indices*/
            val GeodataDTO(latitude, longitude) = location
            Some(GeoHash.withBitPrecision(latitude, longitude, 30).longValue())
    }

    def matchGeo(smaller: (Pipe, Fields, Fields, Fields), larger: (Pipe, Fields, Fields), distance: Fields, metaFields: Fields, measure: SimpleDistanceMeasure = GeographicDistanceMetric("m"), threshold: Int = 50, top: Int = 3) = {
        val (smallerPipe, smallerBlockId, smallerLocation, popularity) = smaller
        val (largerPipe, largerBlockId, largerLocation) = larger
        val blockIds = Fields.merge(smallerBlockId, largerBlockId)
        val groupFields = 'indexEl append blockIds
        val distanceAndMetaFields = distance append metaFields subtract blockIds
        addIndex(smallerPipe, smallerLocation, measure, threshold)
                // join pipes on blocking indices
                .joinWithLarger(('indexEl append smallerBlockId) -> ('indexEl append largerBlockId), addIndex(largerPipe, largerLocation, measure, threshold))
                .map(Fields.ALL ->()) {
            in: Tuple =>
                println("tuple: " + in)

        }
                // filter out locations that are further away than the threshold
                .flatMap((smallerLocation, largerLocation) -> distance) {
            in: (GeodataDTO, GeodataDTO) =>
                val (locationSmaller, locationLarger) = in
                println("fm: " + smallerBlockId + "/ " + in)
                val distance = measure.evaluate(locationSmaller.canonicalLatLng, locationLarger.canonicalLatLng)
                val result = if (distance > threshold) None else Some(distance)
                result
        }.groupBy(groupFields) {
            _.sum(popularity -> 'popularitySum).then {
                /* // normalize data
         _.sizeAveStdev(popularity -> popularityFields)
                 .mapReduceMap((distance) -> (distance)) {
             in: Double =>
                 println("POP " + in)
                 in
         } {
             (left: Double, right: Double) =>
                 println("III" + left + " " + right)
                 left
         }(identity[Double])
                */
                // return top elements
                _.sortWithTake[cascading.tuple.Tuple]((distanceAndMetaFields append ('popularitySum)) -> 'topEls, top) {
                    (left: cascading.tuple.Tuple, right: cascading.tuple.Tuple) => {
                        left.getDouble(0) > right.getDouble(0)
                    }
                }
            }
        }.discard('indexEl).map('popularitySum ->()) {
            in: Tuple =>
                println("XXX" + in)
        }.flatten[Tuple]('topEls -> distanceAndMetaFields).discard('popularitySum, 'topEls)
    }

    def isWeekDay(ldt: LocalDateTime) = WeekDays(ldt.getDayOfWeek)

}
