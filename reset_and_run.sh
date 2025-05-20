#!/bin/bash

hdfs_wd=/user/mohamed/kmeans_spark

# copy input file to hdfs
hdfs dfs -copyFromLocal ./acc/$1 $hdfs_wd

# remove output directory (if exists)
hdfs dfs -rm $hdfs_wd/output/*
hdfs dfs -rmdir $hdfs_wd/output

# remove centroids
hdfs dfs -rm $hdfs_wd/centroids/*
hdfs dfs -rmdir $hdfs_wd/centroids

# run MapReduce
spark-submit --class main.Main \
 --master local[*] ./untitled/target/kmeans_spark-1.0.jar \
hdfs://localhost:9000/$hdfs_wd/$1 \
hdfs://localhost:9000/$hdfs_wd/output \
$2 \
$3 > log.txt

# copy result to local
hdfs dfs -copyToLocal $hdfs_wd/output/part-00000 ./acc

# rename output file, effectively deleting the old output file
mv ./acc/part-00000 ./acc/out.txt

# print execution time
grep "Took" log.txt
