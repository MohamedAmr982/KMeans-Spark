package main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Main {

    public static final String CENTROID_PATH = "/user/mohamed/kmeans_spark/centroids";

    public static void main(String[] args) throws IOException{
        String inputFilePath = args[0];
        String outputFilePath = args[1];
        // data vector length
        String dStr = args[2];
        // number of clusters
        String kStr = args[3];

        int d = Integer.parseInt(dStr);
        int k = Integer.parseInt(kStr);

        SparkConf sparkConfig = new SparkConf()
                .setMaster("local")
                .setAppName("kmeans")
                .set("d", dStr)
                .set("k", kStr);

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);

        // read input file (dataset)
        JavaRDD<String> inputDataset = sparkContext.textFile(inputFilePath);

        long startTime = System.nanoTime();

        initializeCentroids(inputDataset, sparkContext, CENTROID_PATH, d, k);

        final int MAX_ITERATIONS = 50;
        boolean converged = false;
        int i = 0;

        List<Tuple2<Integer, Vector>> centroids = sparkContext
                .textFile(CENTROID_PATH+"/part-00000")
                .mapToPair(
                        line -> {
                            String[] parts = line.split(" ");
                            int clusterID = Integer.parseInt(parts[0]);
                            return new Tuple2<>(
                                    clusterID,
                                    new Vector(d, parts[1], ",")
                            );
                        }
                ).collect();

        while (!converged && i < MAX_ITERATIONS) {

            // map to (closest cluster index, vector)
            List<Tuple2<Integer, Vector>> currentCentroids = centroids;
            JavaPairRDD<Integer, Vector> pairs = inputDataset.mapToPair(
                line -> {
                    Vector vector = new Vector(d, line, ",");
                    // find closest centroid
                    // assign vector to id of closest centroid

                    Tuple2<Integer, Vector> closestCentroid = currentCentroids.get(0);

                    for (final Tuple2<Integer, Vector> t: currentCentroids) {
                        if (vector.squaredDistanceTo(t._2) < vector.squaredDistanceTo(closestCentroid._2)) {
                            closestCentroid = t;
                        }
                    }
                    return new Tuple2<>(closestCentroid._1, vector);
                }
            );

            // cluster_id, vector, 1
            JavaPairRDD<Integer, Tuple2<Vector, Integer>> clusterCounts = pairs.mapValues(v -> new Tuple2(v, 1));

            clusterCounts.reduceByKey(
                    (a, b) ->
                            new Tuple2(a._1.add(b._1), a._2 + a._2)
            );

            JavaPairRDD<Integer, Vector> newCentroidsRDD = clusterCounts.mapValues(x -> x._1.divideBy(x._2));

            List<Tuple2<Integer, Vector>> newCentroids = newCentroidsRDD.collect();

            converged = true;
            // compare old and new centroids
            for (int j = 0; j < k; j++) {
                if (newCentroids.get(j) != centroids.get(j)) {
                    converged = false;
                    break;
                }
            }

            i++;
        }

        List<Tuple2<Integer, Vector>> finalCentroids = centroids;
        JavaPairRDD<Integer, Vector> finalClustering = inputDataset.mapToPair(
                line -> {
                    Vector vector = new Vector(d, line, ",");
                    // find closest centroid
                    // assign vector to id of closest centroid

                    Tuple2<Integer, Vector> closestCentroid = finalCentroids.get(0);

                    for (final Tuple2<Integer, Vector> t: finalCentroids) {
                        if (vector.squaredDistanceTo(t._2) < vector.squaredDistanceTo(closestCentroid._2)) {
                            closestCentroid = t;
                        }
                    }
                    return new Tuple2<>(closestCentroid._1, vector);
                }
        );

        long execTime = System.nanoTime() - startTime;

        System.out.println("Took " + execTime + " ns.");

        finalClustering.saveAsTextFile(outputFilePath);

    }

    public static void initializeCentroids(JavaRDD<String> input, JavaSparkContext sc, String centroidsPath, int d,int k) throws IOException {
        List<String> lines = input.distinct().take(k);
        List<String> newLines = new ArrayList<>();

        int i = 0;

        while (i < k) {
            String line = lines.get(i);
            String[] values = line.split(",");

            // (clusterId, vector)
            StringBuilder newLine = new StringBuilder(i + " ");

            for (int j = 0; j < d; j++) {
                newLine.append(values[j]);
                if (j != d - 1) {
                    newLine.append(",");
                }
            }
            newLines.add(String.valueOf(newLine));
            i++;
        }

        // Convert list to RDD
        JavaRDD<String> rdd = sc.parallelize(newLines);

        // Save to file or HDFS
        rdd.saveAsTextFile(centroidsPath);
    }
}
