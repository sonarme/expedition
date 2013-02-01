package com.sonar.expedition.scrawler.checkins

import de.fuberlin.wiwiss.silk.plugins.metric.GeographicDistanceMetric
import cascading.pipe.Pipe
import com.sonar.expedition.scrawler.pipes.ScaldingImplicits
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
import cascading.tuple.Fields
import com.sonar.dossier.dto.GeodataDTO
import ch.hsr.geohash.GeoHash

trait CheckinInference extends ScaldingImplicits {
    val GeoSectorSizeInBits = 30


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
            Some(GeoHash.withBitPrecision(latitude, longitude, GeoSectorSizeInBits).longValue())
    }

    def matchGeo(venues: (Pipe, Fields, Fields, Fields), checkins: (Pipe, Fields, Fields, Fields), distance: Fields, metaFields: Fields, measure: SimpleDistanceMeasure = GeographicDistanceMetric("m"), threshold: Int = 200, top: Int = 3) = {
        val (venuesPipe, venuesBlockId, venuesLocation, popularity) = venues
        val (checkinsPipe, checkinsBlockId, checkinsLocation, checkinId) = checkins
        // fields that should remain in the pipe after the matching
        val distanceAndMetaFields = Fields.merge(distance append popularity, metaFields)
        val venuesPipeWithBlocking = addIndex(venuesPipe, venuesLocation, measure, threshold)
        val checkinsPipeWithBlocking = addIndex(checkinsPipe, checkinsLocation, measure, threshold)
        venuesPipeWithBlocking
                // join pipes on blocking indices
                .joinWithLarger(('indexEl append venuesBlockId) -> ('indexEl append checkinsBlockId), checkinsPipeWithBlocking)
                // filter out locations that are further away than the threshold
                .flatMap((venuesLocation, checkinsLocation) -> distance) {
            in: (GeodataDTO, GeodataDTO) =>
                val (venuesLocation, checkinLocation) = in
                val distance = measure.evaluate(venuesLocation.canonicalLatLng, checkinLocation.canonicalLatLng)
                val result = if (distance > threshold) None else Some(distance)
                //println("Blocking match: id:" + venuesBlockId + " / geodata: " + in + " / distance: " + distance + " / result: " + result)
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

                //println("Popularity: " + popularitySum + " /  Top elements: " + topEls)
                topEls
        }.discard('popularitySum, 'topEls)
    }

}
