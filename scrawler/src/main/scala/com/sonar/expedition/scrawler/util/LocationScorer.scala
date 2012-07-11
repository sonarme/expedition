package com.sonar.expedition.scrawler.util

class LocationScorer {
    val levver = new Levenshtein
    val havver = new Haversine

    def getScore(clustName: String, clustLat: String, clustLng: String, placeName: String, placeLat: String, placeLng: String): Double = {
       val levDistance = levver.compareInt(clustName, placeName)
       val havDistance = havver.haversine(clustLat.toDouble, clustLng.toDouble, placeLat.toDouble, placeLng.toDouble)
       (levDistance+1) * (havDistance+1)
    }

}

object LocationScorer {

}
