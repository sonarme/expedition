package com.sonar.expedition.scrawler.clustering

import org.apache.mahout.math.DenseVector
import org.apache.mahout.clustering.kmeans.{KMeansClusterer, Cluster}
import org.apache.mahout.common.distance.EuclideanDistanceMeasure
import collection.JavaConversions._

object KMeansClustering {

    def clusterCenterAsString(points: Iterable[String], k: Int) = {
        val (lat, lng) = clusterCenter(points.map {
            point =>
                val Array(pointLat, pointLng) = point.split(':')
                (pointLat.toDouble, pointLng.toDouble)
        }, k)
        lat + ":" + lng
    }

    def clusterCenter(points: Iterable[(Double, Double)], k: Int) = {
        val highest = cluster(points map {
            case (lat, lng) => Array(lat, lng)
        }, k)
        (highest.getCenter.get(0), highest.getCenter.get(1))
    }


    def cluster(points: Iterable[Array[Double]], k: Int) = {
        require(k > 0)
        val pointsAsVectors = points map {
            arr => new DenseVector(arr)
        }
        val initialClusters = pointsAsVectors.take(k).zipWithIndex.map {
            case (vector, idx) => new Cluster(vector, idx)
        }
        val finalClusters = KMeansClusterer.clusterPoints(pointsAsVectors.toSeq, initialClusters.toSeq, new EuclideanDistanceMeasure, 5, 0.01)
        finalClusters.last.maxBy(_.getNumPoints)

    }
}
