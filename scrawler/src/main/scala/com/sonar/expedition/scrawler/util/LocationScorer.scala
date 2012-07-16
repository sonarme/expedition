package com.sonar.expedition.scrawler.util

class LocationScorer extends Serializable {
    val levver = new Levenshtein
    val havver = new Haversine

    def getScore(workName: String, workLat: String, workLng: String, placeName: String, placeLat: String, placeLng: String): Tuple2[Double, Double] = {
        var levDistance = 0.0
        var havDistance = 0.0
        if (placeLat == null) {
            havDistance = -1.0
        }
        else {
            havDistance = havver.haversine(workLat.toDouble, workLng.toDouble, placeLat.toDouble, placeLng.toDouble)
        }
        if (placeName == null) {
            levDistance = -1.0
        }
        else {
            levDistance = levver.compareInt(workName, placeName)
        }
        (levDistance, havDistance)
    }

    def certaintyScore(scores: Tuple2[Double, Double], workName: String, placeName: String): Double = {
        var certainty = 0.0
        if (placeName == null) {
            certainty.+(-3.15)
        }
        else if (workName == placeName) {
            certainty = certainty + 10
            if (scores._2 >= (0.0) && scores._2 <= (2.0)) {
                certainty = certainty + 10.0 + (scores._2 * (-1.0))
                certainty
            }
            else {
                certainty = certainty + (scores._2 * (-1.0))
                certainty
            }
        }
        else {
            if (scores._1 >= (0.0) && scores._1 <= (2.0)) {
                certainty = certainty + 10.0
                if (scores._2 >= (0.0) && scores._2 <= (2.0)) {
                    certainty = certainty + 10.0 + (scores._1 * (-1.0)) + (scores._2 * (-1.0))
                    certainty
                }
                else {
                    certainty = certainty + (scores._1 * (-1.0)) + (scores._2 * (-1.0))
                    certainty
                }
            }
            else if (scores._2 >= (0.0) && scores._2 <= (2.0)) {
                certainty = certainty + 10.0 + (scores._1 * (-1.0)) + (scores._2 * (-1.0))
                certainty
            }
            else {
                certainty.+(scores._1.*(-1.0)).+(scores._2.*(-1.0))
            }
        }
    }


}

object LocationScorer {

}
