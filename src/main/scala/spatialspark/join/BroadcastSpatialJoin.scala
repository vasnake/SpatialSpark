/*
 * Copyright 2015 Simin You
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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import spatialspark.operator.SpatialOperator
import spatialspark.join.broadcast.index._

object BroadcastSpatialJoin {

  private
  def queryRtree(rtreeIndex: Broadcast[RtreeIndex],
                 leftObj: Row, leftGeom: Geometry,
                 predicate: SpatialOperator.SpatialOperator,
                 condition: Option[(Row, Row) => Boolean]
                ): Array[(Row, Row, Geometry, Geometry)] = {
    def result(rightObj: Row, rightGeom: Geometry) = (leftObj, rightObj, leftGeom, rightGeom)
    def getres(lst: Array[FeatureExt]) = lst.map(f => result(f.feature.row, f.feature.geometry))

    val queryBox = leftGeom.getEnvelopeInternal

    // candidates for case with intersect-ish relation
    lazy val candidates = {
      val features = rtreeIndex.value.featuresInEnvelope(queryBox).toArray
      val filtered = condition match {
        case Some(cond) => features.filter(el => cond(leftObj, el.feature.row))
        case _ => features
      }

      filtered
    }

    if (predicate == SpatialOperator.Within)
      getres(candidates.filter { f => leftGeom.within(f.feature.geometry) })
    else if (predicate == SpatialOperator.Contains)
      getres(candidates.filter { f => leftGeom.contains(f.feature.geometry) })
    else if (predicate == SpatialOperator.Intersects)
      getres(candidates.filter { f => leftGeom.intersects(f.feature.geometry) })
    else if (predicate == SpatialOperator.Overlaps)
      getres(candidates.filter { f => leftGeom.overlaps(f.feature.geometry) })
    // special case: buffered envelop should produce candidates
    else if (predicate == SpatialOperator.WithinD)
      getres(candidates.filter { f => leftGeom.isWithinDistance(f.feature.geometry, f.delta) })
    // special case: rtree.nearest using distance(g1, g2) function
    // TODO: apply condition fiter!
    else if (predicate == SpatialOperator.NearestD && rtreeIndex.value.size() > 0) {
      val item = rtreeIndex.value
          .nearestNeighbour(queryBox, FeatureExt(Row.empty, leftGeom, delta=0), distanceImpl)
      Array(result(item.feature.row, item.feature.geometry))
    }
    // unknown predicate
    else {
      Array.empty[(Row, Row, Geometry, Geometry)]
    }

  }

  /**
    * For each record in left find all records in right, that satisfy joinPredicate and condition.
    * Geometries should be Lon,Lat-based.
    *
    * зачем нужны Row на входе;
    * зачем нужны Row, Geometry на выходе;
    * зачем нужен condition;
    * что происходит с радиусом, ограничения на геометрию (lon,lat only)
    *
    * @param sc            context
    * @param left          big dataset for iterate over (flatmap)
    * @param right         small dataset, will be broadcasted
    * @param joinPredicate spatial relation, e.g. contain, intersects, ...
    * @param radius        meters, used in withinD relation: leftGeom.isWithinDistance(rightGeom, radius)
    * @param condition     optional function to check left-right attributes
    * @return inner join result: (left, right, left_geom, right_geom)
    */
  def apply(sc: SparkContext,
            left: RDD[(Row, Geometry)],
            right: RDD[(Row, Geometry)],
            joinPredicate: SpatialOperator.SpatialOperator,
            radius: Double = 0,
            condition: Option[(Row, Row) => Boolean] = None
           ): RDD[(Row, Row, Geometry, Geometry)] = {

    val rtreeBcast = sc.broadcast(buildRtree(right, radius))

    left
        .filter(rec => rec._2 != null) // check left geometry
        .flatMap { rec => queryRtree(rtreeBcast, rec._1, rec._2, joinPredicate, condition)
    }
  }

  // TODO: check if serializable
  private val distanceImpl = new JTSDistanceImpl()

  // lon,lat data adaptation to JTS Euclidian geometry
  private val degreesInMeter = 1d / 111200d
  private val pi180 = math.Pi / 180d

  private def metre2degree(metre: Double, lon: Double): Double = {
    (metre * degreesInMeter) / math.cos(lon * pi180)
  }

  private def buildRtree(rdd: RDD[(Row, Geometry)], radius: Double): RtreeIndex = {

    val featuresWithEnvelopes = rdd
        .filter(r => r._2 != null) // check geometry
        .map { rec =>
          val envelope = rec._2.getEnvelopeInternal

          if (radius != 0) {
            // convert distance in meters to arc length in degrees along latitude axis, in respect to longitude
            val delta = metre2degree(radius, envelope.centre.y)
            // make shure index won't miss that envelope
            envelope.expandBy(delta)
            // save converted distance to future use
            (envelope, FeatureExt(rec, delta))
          }
          else (envelope, FeatureExt(rec, 0))
        }

    RtreeIndex(featuresWithEnvelopes.collect())
  }

}
