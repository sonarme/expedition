package com.sonar.expedition.scrawler.pipes

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings

class GeosectorTest extends FlatSpec with ShouldMatchers {
    "geosector..." should "..." in {
        val sectorLength = 20
        val geohash = GeoHash.withBitPrecision(40.0, -71.0, sectorLength)
        val adjacents = geohash.getAdjacent
        val adjacentGeoHashLongValues = adjacents.map(_.longValue())
        val placeGeohash = GeoHash.withBitPrecision(40.0, -74.0, sectorLength).longValue()
        assert(adjacentGeoHashLongValues contains placeGeohash)
    }
}
