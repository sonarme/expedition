package com.sonar.expedition.scrawler.util

import net.sf.javaml.core.{Instance, DenseInstance, DefaultDataset}
import net.sf.javaml.clustering.OPTICS
import scala.Array
import net.sf.javaml.distance.DistanceMeasure
import collection.JavaConversions._

object LocationClusterer {
    val Epsilon = 50
    val MinPoints = 4
    val clusterer = new OPTICS(Epsilon, MinPoints, HaversineDistanceMeasure)

    def cluster(points: Iterable[(Double, Double)]) = {
        val instances = points map {
            case (lat, lng) => new DenseInstance(Array(lat, lng))
        }
        clusterer.cluster(new DefaultDataset(instances))
    }

    def maxClusterCenter(points: Iterable[(Double, Double)]) = {
        val clusters = cluster(points)
        if (clusters.isEmpty) None
        else {
            val max = cluster(points).maxBy(_.length)
            val avg = average(max.map(instance => (instance.value(0), instance.value(1))))
            Some(avg)
        }
    }

    /**
     * http://stackoverflow.com/a/8564922
     * @param points
     * @return
     */
    def average(points: Iterable[(Double, Double)]) = {
        val lonDegreesTotal = points.map(_._2).sum
        val latRadians = points.map(_._1 * math.Pi / 180)
        val latXTotal = latRadians.map(math.cos).sum
        val latYTotal = latRadians.map(math.sin).sum
        val finalLatRadians = math.atan2(latYTotal, latXTotal)
        val finalLatDegrees = finalLatRadians * 180 / math.Pi
        val finalLonDegrees = lonDegreesTotal / points.size
        (finalLatDegrees, finalLonDegrees)
    }
}

object HaversineDistanceMeasure extends DistanceMeasure {
    def measure(x: Instance, y: Instance) = Haversine.haversineInMeters(x.value(0), x.value(1), y.value(0), y.value(1))

    def compare(x: Double, y: Double) = x > y
}
