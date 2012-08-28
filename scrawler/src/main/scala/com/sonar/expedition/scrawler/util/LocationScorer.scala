package com.sonar.expedition.scrawler.util

object LocationScorer extends Serializable {

    def getScore(workName: String, workLat: String, workLng: String, placeName: String, placeLat: String, placeLng: String) = {
        val havDistance =
            if (placeLat == null) -1.0 else Haversine.haversine(workLat.toDouble, workLng.toDouble, placeLat.toDouble, placeLng.toDouble)
        val levDistance =
            if (placeName == null) -1.0 else Levenshtein.compareInt(workName, placeName)
        (levDistance, havDistance)
    }

    def certaintyScore(scores: (Double, Double), workName: String, placeName: String): Double =
        if (placeName == null)
            -3.15
        else if (workName == placeName) {
            if (scores._2 >= 0 && scores._2 <= 2.0) 20 - scores._2 else 10 - scores._2
        }
        else {
            if (scores._1 >= 0.0 && scores._1 <= 2.0) {
                if (scores._2 >= 0.0 && scores._2 <= 2.0) 20 - scores._1 - scores._2 else 10 - scores._1 - scores._2
            }
            else if (scores._2 >= 0.0 && scores._2 <= 2.0) 10 - scores._1 - scores._2 else -scores._1 - scores._2
        }


}
