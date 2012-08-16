package com.sonar.expedition.scrawler.clustering;

import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansClusterer;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.util.ArrayList;
import java.util.List;


// does clustering to find home and work locations
public class KMeans {

    private Vector[] randomPoints;

    public KMeans() {
    }

    public String clusterKMeans(List<String> chkins, int clustersReq) {
        int k = clustersReq;
        List<Vector> sampleData = new ArrayList<Vector>();
        List<geopoints> points = new ArrayList<geopoints>();
        List<Cluster> clusters = new ArrayList<Cluster>();
        int clusterId = 0;

        for (String loc : chkins) {
            String[] locs = loc.split(":");
            points.add(new geopoints(locs[0], locs[1]));
        }
        for (int i = 0; i < points.size(); i++) {
            DenseVector v = new DenseVector(new double[]{
                    points.get(i).getLat(), points.get(i).getLon()});
            sampleData.add(v);
            if (i < k)
                clusters.add(new Cluster(v, clusterId++));
        }

        List<List<Cluster>> finalClusters = KMeansClusterer.clusterPoints(
                sampleData, clusters, new EuclideanDistanceMeasure(), 5, 0.01);
        String loc = "0:0";
        int numOfPoints = 0;
        for (Cluster cluster : finalClusters.get(finalClusters.size() - 1)) {
            if (cluster.getNumPoints() > numOfPoints) {
                numOfPoints = cluster.getNumPoints();
                loc = cluster.getCenter().get(0) + ":" + cluster.getCenter().get(1);
            }
        }
        return loc;
    }

    public String clusterDirichlet(List<String> chkins, int clustersReq) {
        /*int k = clustersReq;
        List<Vector> sampleData = new ArrayList<Vector>();
        List<geopoints> points = new ArrayList<geopoints>();

        for (String loc : chkins) {
            String[] locs = loc.split(":");
            points.add(new geopoints(locs[0], locs[1]));
        }
        for (int i = 0; i < points.size(); i++) {

            sampleData.add(new DenseVector(new double[]{
                    points.get(i).getLat(), points.get(i).getLon()}));
        }
        List<Cluster> clusters = new ArrayList<Cluster>();
        int clusterId = 0;
        for (Vector v : sampleData) {
            clusters.add(new Cluster(v, clusterId++));
        }

        DirichletClusterer dc = new DirichletClusterer(sampleData, NormalModelDistribution(), alpha0, numClusters, thin, burnin);

        List<Cluster[]> result = dc.cluster(numIterations);
        printModels(result, burnin);
        for (Cluster[] models : result) {
            List<Cluster> clusters = new ArrayList<Cluster>();
            for (Cluster cluster : models) {
                if (isSignificant(cluster)) {

                    String loc = "0:0";
        int numOfPoints = 0;
        for (Cluster cluster : finalClusters.get(finalClusters.size() - 1)) {
            if (cluster.getNumPoints() > numOfPoints) {
                numOfPoints = cluster.getNumPoints();
                loc = cluster.getCenter().get(0) + ":" + cluster.getCenter().get(1);
            }
        } */

        return "";
    }


}

class geopoints {
    double lat;
    double lon;

    public geopoints(double lat, double lon) {
        super();
        this.lat = lat;
        this.lon = lon;
    }

    public geopoints(String lat, String lon) {
        super();
        this.lat = Double.parseDouble(lat);
        this.lon = Double.parseDouble(lon);
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }


}
