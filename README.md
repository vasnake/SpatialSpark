# Big Spatial Data Processing using Spark

[![Join the chat at https://gitter.im/syoummer/SpatialSpark](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/syoummer/SpatialSpark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/syoummer/SpatialSpark.svg?branch=master)](https://travis-ci.org/syoummer/SpatialSpark)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/me.simin/spatial-spark_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/me.simin/spatial-spark_2.11)

## Introduction

[SpatialSpark](http://simin.me/projects/spatialspark/) aims to provide efficient spatial operations using Apache Spark. It can be used as a Spark library for
spatial extension as well as a standalone application to process large scale spatial join operations. 

SpatialSpark has been compiled and tested on Spark 2.0.2. For geometry operations and data structures for indexes, well known [JTS](http://www.vividsolutions.com/jts/JTSHome.htm) library is used.

## Usage 

### Library
SpatialSpark is published to Maven Central (including SNAPSHOT releases). Just use following coordinates with your build tool:

```XML
<dependency>
    <groupId>me.simin</groupId>
    <artifactId>spatial-spark_2.11</artifactId>
    <version>1.1.1</version>
</dependency>
```

### DataFrame Implementation
working in progress (preliminary implementation under `exp`)  

### Standalone Application
If you want, you can also use a few predefined Spark jobs.
To get jar for standalone app, simply use SBT:

    sbt
    sbt> standalone/assembly

Then, you can run SpatialJoinApp on your local machine:

    java -Dspark.master=local[4] \
        -cp standalone/target/scala-2.11/standalone-assembly-1.1.2.jar \
        spatialspark.main.SpatialJoinApp \
        --left data/point1k.tsv --geom_left 1 \
        --right data/nycb.tsv --geom_right 0 \
        --broadcast true \
        --predicate within \
        --output target/join_output \
        --partition 4 \
        --num_output 1

Or, if you prefer to use `spark-submit`, build another jar:

    sbt> assembly

Then, you can use `spark-submit` to submit a Spark job:

    spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --class spatialspark.main.SpatialJoinApp \
      /app/scala-2.11/spatial-spark-assembly-1.1.2.jar \
      --left /data/point1k.tsv --geom_left 1 \
      --right /data/nycb.tsv --geom_right 0 \
      --output /app/join_output \
      --broadcast true \
      --predicate within \
      --partition 4 \
      --num_output 1

You can use predefined Docker containers to run tests. Go to `docker` subdir and start a container:

    docker$ sudo docker-compose up
    sudo docker exec -it spark-worker-1 /bin/bash

This should start a command shell where you can use `spark-submit` command.
Try to run previously described job for example.

A few useful snippets for container management:

    sudo docker-compose down --volumes

    # docker root problem workaround for generated files
    docker$ setfacl -m "default:group::rwx" "../target/join_output"

    # remove docker containers and images
    sudo docker ps -q -a | xargs sudo docker stop
    sudo docker ps -q -a | xargs sudo docker rm
    sudo docker image rm $(sudo docker image ls -a -q)

#### Spatial Join
Run spatial join processing on two datasets with spatial information (in 
[WKT](http://en.wikipedia.org/wiki/Well-known_text) format) Use `--help` to list all available options.

*Example*  
Assuming two input datasets (A and B) have been uploaded to HDFS (or S3 for Amazon AWS), below is a broadcast based
spatial join example.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    spatial-spark-assembly-1.1.0-SNAPSHOT.jar --left A --geom_left 0 --right B --geom_right 0 --broadcast true --output output \
    --partition 1024 --predicate within 
    
If both datasets are very large, so that the right dataset cannot fit in memory, here is an example of performing
partition based spatial join.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    spatial-spark-assembly-1.1.0-SNAPSHOT.jar --left A --geom_left 0 --right B --geom_right 1 --broadcast false --output output \
    --partition 1024 --predicate within --method stp --conf 32:32:0.1 --parallel_part true

We have provided two sample datasets, including one point dataset (`data/point1k.tsv`) and one polygon
dataset (`data/nycb.tsv`).

For broadcast based spatial join, use

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    spatial-spark-assembly-1.1.0-SNAPSHOT.jar --left data/point1k.tsv --geom_left 1 --right data/nycb.tsv --geom_right 0 \
    --broadcast true --output output --predicate within

For partition based spatial join with STP, use

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    spatial-spark-assembly-1.1.0-SNAPSHOT.jar --left data/point1k.tsv --geom_left 1 --right data/nycb.tsv --geom_right 0 \
    --broadcast false --output output --predicate within --method stp --conf 32:32:0.1 \
    --parallel_part false

#### Spatial Partition
Generate a spatial partition from input dataset, currently Fixed-Grid Partition (FGP), Binary-Split Partition (BSP) and
Sort-Tile Partition (STP) are supported. Use `--help` to list all options.

#### Spatial Range Query
Spatial range query includes both indexed and non-indexed query. For non-indexed query, a full scan is performed on the
dataset and returns filtered results. here is an example,

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.Query \
    spatial-spark-assembly-1.1.0-SNAPSHOT.jar --input data/point1k.tsv --geom 1 --output output.tsv \
    --query 98500.0,181800.0,986000.0,182000.0

Since a full scan needs to load the whole dataset, the performance may be bad if the dataset is very large. To improve
the performance, an indexed range query is supported.

Before performing the indexed range query, an index need to be created. An example is shown below.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.Index \
    spatial-spark-assembly-1.1.0-SNAPSHOT.jar --input data/point1k.tsv --geom 1 --output data/point1k_new \
    --conf 32:32:0.3

After the job, the dataset will be re-ordered and saved in the specified output location and an index file will
be created in the specified output location with `"_index"` suffix. The index file is separated with the data, and the
content of the new dataset is as same as the orignal one but in different order.

With created index, the range query can be performed very fast.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.Query \
    spatial-spark-assembly-1.1.0-SNAPSHOT.jar --input data/point1k.tsv --geom 1 --output output.tsv \
    --query 98500.0,181800.0,986000.0,182000.0 --use_index true

## Version history
[changelog](CHANGELOG.md)

## Future Work
* Add tests
* More documentation
* Spatial indexed range query using R-tree and Grid-file
* KNN search

## Contact
If you have questions and comments, use [Gitter chat](https://gitter.im/syoummer/SpatialSpark) or contact [me](http://simin.me).

    Copyright 2015 Simin You
    Copyright 2015 Kamil Gorlo

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
