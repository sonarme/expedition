package com.sonar.expedition.scrawler

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import util.{LocationClusterer, Haversine}

class LocationClustererTest extends FlatSpec with ShouldMatchers {

    def readFile(file: String) = io.Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().toSeq.map {
        s =>
            val Array(lat, lng) = s.split(':')
            (lat.toDouble, lng.toDouble)
    }

    val PaulsWorkdayPongs = readFile("/paulwork.txt")
    val PaulsHomePongs = readFile("/paulhome.txt")
    "pauls workday pongs" should "cluster to sonar hq" in {
        val (lat, lng) = LocationClusterer.maxClusterCenter(PaulsWorkdayPongs)
        assert(Haversine.haversineInMeters(40.7455316, -73.9840987, lat, lng) < 50)
    }
    "pauls home pongs" should "cluster to his pad" in {
        val (lat, lng) = LocationClusterer.maxClusterCenter(PaulsHomePongs)
        assert(Haversine.haversineInMeters(40.666707, -73.975925, lat, lng) < 50)
    }

}

