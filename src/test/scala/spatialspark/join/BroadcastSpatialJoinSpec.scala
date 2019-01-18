/*
 * Copyright 2015 Kamil Gorlo
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package spatialspark.join

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import spatialspark.operator.SpatialOperator._

class BroadcastSpatialJoinSpec extends SparkSpec with GeometryFixtures with SpatialJoinBehaviors {

  def broadcastSpatialJoinAlgorithm = new SpatialJoinAlgorithm {

    override def run(firstGeomWithId: RDD[(Long, Geometry)],
                     secondGeomWithId: RDD[(Long, Geometry)],
                     predicate: SpatialOperator): List[(Long, Long)] = {

      BroadcastSpatialJoin(sc, firstGeomWithId, secondGeomWithId, predicate)
          .map { case (left, right, _, _) => (left, right)}
          .collect.toList
    }

  }

  behavior of "BroadcastSpatialJoin algorithm"

  it should behave like spatialJoinAlgorithm(broadcastSpatialJoinAlgorithm)

//  override protected def withFixture(test: Any): Outcome = ???
}

// TODO: test all predicates, test WithinD variations, test standalone, test spark-submit
/*
java -Dspark.master=local[4] \
    -cp standalone/target/scala-2.11/standalone-assembly-1.1.2-SNAPSHOT.jar \
    spatialspark.main.SpatialJoinApp \
    --left data/point1k.tsv --geom_left 1 \
    --right data/nycb.tsv --geom_right 0 \
    --broadcast true \
    --predicate within \
    --output target/join_output \
    --partition 4 \
    --num_output 1
 */
