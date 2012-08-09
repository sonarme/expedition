package com.sonar.expedition.scrawler.pipes

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings

class GeosectorTest extends FlatSpec with ShouldMatchers {
    "adjacent geosectors" should "contain the given place" in {
        val sectorLength = 22
        val geohash = GeoHash.withBitPrecision(40.734771, -73.990173, sectorLength)
        val adjacents = geohash.getAdjacent
        val adjacentGeoHashLongValues = adjacents.map(_.longValue())
        val placeGeohash = GeoHash.withBitPrecision(40.746217, -74.026566, sectorLength).longValue()
        assert(adjacentGeoHashLongValues contains placeGeohash)
    }
}
