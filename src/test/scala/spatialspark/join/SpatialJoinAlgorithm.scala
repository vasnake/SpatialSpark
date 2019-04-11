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

import org.locationtech.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import spatialspark.operator.SpatialOperator._

// TODO: use not only in tests?
trait SpatialJoinAlgorithm {

  def run(firstGeomWithId: RDD[(Long, Geometry)],
          secondGeomWithId: RDD[(Long, Geometry)],
          predicate: SpatialOperator): List[(Long, Long)]

}
