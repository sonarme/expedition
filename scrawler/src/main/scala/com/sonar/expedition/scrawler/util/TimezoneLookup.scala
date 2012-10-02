package com.sonar.expedition.scrawler.util

import ch.hsr.geohash.GeoHash
import collection.JavaConversions._
import org.joda.time.DateTimeZone

object TimezoneLookup {
    val TimezoneRegex = """(.+)\s+(.+)\s+(.+)""".r

    /**
     * data from http://stackoverflow.com/questions/5584602/determine-timezone-from-latitude-longitude-without-using-web-services-like-geona#comment11570232_5584826
     * and removed the few timezones that DateTimeZone can't handle
     */
    private lazy val timezones = new java.util.TreeMap[Long, DateTimeZone](dataFileMap("/datafiles/timezones.txt"): java.util.Map[Long, DateTimeZone])

    def dataFileMap(file: String) = io.Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().flatMap {
        _ match {
            case TimezoneRegex(lat, lng, tz) => Some((geohash(lat.toDouble, lng.toDouble), DateTimeZone.forID(tz.trim)))
            case _ => None
        }
    }.toMap[Long, DateTimeZone]

    private def geohash(lat: Double, lng: Double) = GeoHash.withCharacterPrecision(lat, lng, 6).longValue()

    def getClosestTimeZone(lat: Double, lng: Double) = {
        val geohashKey = geohash(lat, lng)
        val higher = timezones.higherEntry(geohashKey)
        val lower = timezones.lowerEntry(geohashKey)
        val min = Seq(higher, lower).minBy(entry => math.abs(entry.getKey - geohashKey))
        min.getValue
    }
}
