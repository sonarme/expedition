package com.sonar.expedition.scrawler.clustering

import org.apache.mahout.math.DenseVector
import org.apache.mahout.clustering.kmeans.{KMeansClusterer, Cluster}
import org.apache.mahout.common.distance.EuclideanDistanceMeasure
import collection.JavaConversions._

object KMeansClustering {
    def clusterAsString(points: Iterable[String], k: Int) = {
        val (lat, lng) = cluster(points.map {
            point =>
                val Array(pointLat, pointLng) = point.split(':')
                (pointLat.toDouble, pointLng.toDouble)
        }, k)
        lat + ":" + lng
    }

    def cluster(points: Iterable[(Double, Double)], k: Int) = {
        require(k > 0)
        val pointsAsVectors = points map {
            case (lat, lng) => new DenseVector(Array(lat, lng))
        }
        val initialClusters = pointsAsVectors.take(k).zipWithIndex.map {
            case (vector, idx) => new Cluster(vector, idx)
        }
        val finalClusters = KMeansClusterer.clusterPoints(pointsAsVectors.toSeq, initialClusters.toSeq, new EuclideanDistanceMeasure, 5, 0.01)
        val highest = finalClusters.last.maxBy(_.getNumPoints)
        (highest.getCenter.get(0), highest.getCenter.get(1))
    }
}
