package com.sonar.expedition.scrawler.util

import math._
import ch.hsr.geohash.WGS84Point

//http://rosettacode.org/wiki/Haversine_formula#Scala

object Haversine extends Serializable {
    val R = 6372.8 //earth radius in km

    def haversineInKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
        val dLat = (lat2 - lat1).toRadians
        val dLon = (lon2 - lon1).toRadians

        val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
        val c = 2 * asin(sqrt(a))
        R * c
    }

    def haversineInMeters(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = (haversineInKm(lat1, lon1, lat2, lon2) * 1000).toInt

    def distanceInMeters(p1: WGS84Point, p2: WGS84Point) = haversineInKm(p1.getLatitude, p1.getLongitude, p2.getLatitude, p2.getLongitude) * 1000

}

