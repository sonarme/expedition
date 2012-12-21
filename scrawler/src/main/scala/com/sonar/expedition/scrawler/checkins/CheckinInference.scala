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


    /**
     * add a geo index to the given pipe for the location field with the given distance measure
     */
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

    def matchGeo(venues: (Pipe, Fields, Fields, Fields), checkins: (Pipe, Fields, Fields, Fields), distance: Fields, metaFields: Fields, measure: SimpleDistanceMeasure = GeographicDistanceMetric("m"), threshold: Int = 100, top: Int = 3) = {
        val (venuesPipe, venuesBlockId, venuesLocation, popularity) = venues
        val (checkinsPipe, checkinsBlockId, checkinsLocation, checkinId) = checkins
        // fields that should remain in the pipe after the matching
        val distanceAndMetaFields = Fields.merge(distance append popularity, metaFields)
        println("DAMF" + distanceAndMetaFields)
        println("LOC" + checkinsLocation)
        val venuesPipeWithBlocking = addIndex(venuesPipe, venuesLocation, measure, threshold)
        val checkinsPipeWithBlocking = addIndex(checkinsPipe, checkinsLocation, measure, threshold)
        venuesPipeWithBlocking
                // join pipes on blocking indices
                .joinWithLarger(('indexEl append venuesBlockId) -> ('indexEl append checkinsBlockId), checkinsPipeWithBlocking)
                .map(Fields.ALL ->()) {
            in: Tuple =>
                println("XX: " + in)
        }
                // filter out locations that are further away than the threshold
                .flatMap((venuesLocation, checkinsLocation) -> distance) {
            in: (GeodataDTO, GeodataDTO) =>
                val (venuesLocation, checkinLocation) = in
                println("fm: " + venuesBlockId + "/ " + in)
                val distance = measure.evaluate(venuesLocation.canonicalLatLng, checkinLocation.canonicalLatLng)
                val result = if (distance > threshold) None else Some(distance)
                result
        }.groupBy(checkinId) {
            // add total popularity within georange
            _.sum(popularity -> 'popularitySum)
                    // keep top closest elements in georange
                    .sortWithTake[cascading.tuple.Tuple](distanceAndMetaFields -> 'topEls, top) {
                (left: cascading.tuple.Tuple, right: cascading.tuple.Tuple) => {
                    left.getDouble(0) > right.getDouble(0)
                }
            }
        }.flatMap(('popularitySum, 'topEls) -> distanceAndMetaFields) {
            in: (Long, List[cascading.tuple.Tuple]) =>
                val (popularitySum, topEls) = in
                // make popularity of venues relative to other venues within georange
                topEls foreach {
                    tuple =>
                        cascading.tuple.Tuples.asModifiable(tuple).setDouble(1, tuple.getLong(1) / popularitySum.toDouble)
                }

                println("XXX" + popularitySum + " /    " + topEls)
                topEls
        }.discard('popularitySum, 'topEls)
    }

    def isWeekDay(ldt: LocalDateTime) = WeekDays(ldt.getDayOfWeek)

}
