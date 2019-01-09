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

import java.util
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.strtree.{ItemBoundable, ItemDistance, STRtree}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import spatialspark.operator.SpatialOperator
import org.apache.spark.sql.Row
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.strtree.{ItemDistance, STRtree}
import scala.collection.JavaConversions._

object BroadcastSpatialJoin {

  protected
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

    val joinedItems =
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

    joinedItems
  }

  /**
    * For each record in left find all records in right, that satisfy joinPredicate and condition.
    * Geometries should be Lon,Lat-based.
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

  private val distanceImpl = new JTSDistanceImpl()

  // lon,lat data adaptation to JTS Euclidian geometry
  private val degreesInMeter = 1d / 111200d
  private val pi180 = math.Pi / 180d
  private def metre2degree(metre: Double, lon: Double): Double = {
    (metre * degreesInMeter) / math.cos(lon * pi180)
  }

  private def buildRtree(rdd: RDD[(Row, Geometry)], radius: Double): RtreeIndex = {

    val featuresWithEnvelopes = rdd
        .filter(r => r._2 != null)
        .map { rec =>
          val envelope = rec._2.getEnvelopeInternal

          if (radius != 0) {
            val delta = metre2degree(radius, envelope.centre.y)
            envelope.expandBy(delta)
            (envelope, FeatureExt(rec, delta))
          }
          else (envelope, FeatureExt(rec, 0))
        }

    RtreeIndex(featuresWithEnvelopes.collect())
  }

}

case class Feature(row: Row, geometry: Geometry)
case class FeatureExt(feature: Feature, delta: Double)

object FeatureExt {

  def apply(row: Row, geometry: Geometry, delta: Double): FeatureExt =
    FeatureExt(Feature(row, geometry), delta)

  def apply(row_geom: (Row, Geometry), delta: Double): FeatureExt =
    FeatureExt(Feature(row_geom._1, row_geom._2), delta)
}

case class RtreeIndex(jtsRTree: STRtree) {

  def featuresInEnvelope(envelope: Envelope): Iterable[FeatureExt] =
    jtsRTree.query(envelope).asInstanceOf[util.ArrayList[FeatureExt]]

  def nearestNeighbour(envelope: Envelope, feature: FeatureExt, distanceImpl: ItemDistance): FeatureExt =
    jtsRTree.nearestNeighbour(envelope, feature, distanceImpl).asInstanceOf[FeatureExt]

  def size(): Int = jtsRTree.size()

}

object RtreeIndex {
  def apply(features: Traversable[(Envelope, FeatureExt)]): RtreeIndex = {
    val rtree = new STRtree()
    features.foreach(f => rtree.insert(f._1, f._2))

    RtreeIndex(rtree)
  }
}

class JTSDistanceImpl extends ItemDistance {
  override def distance(item1: ItemBoundable, item2: ItemBoundable): Double = {
    val geom1 = item1.getItem.asInstanceOf[FeatureExt].feature.geometry
    val geom2 = item2.getItem.asInstanceOf[FeatureExt].feature.geometry
    geom1.distance(geom2)
  }
}

object BroadcastSpatialJoin_ {


  def queryRtree(rtree: => Broadcast[STRtree], leftId: Long, geom: Geometry, predicate: SpatialOperator.SpatialOperator,
                 radius: Double): Array[(Long, Long)] = {
    val queryEnv = geom.getEnvelopeInternal
    //queryEnv.expandBy(radius)
    lazy val candidates = rtree.value.query(queryEnv).toArray //.asInstanceOf[Array[(Long, Geometry)]]
    if (predicate == SpatialOperator.Within) {
      candidates.filter { case (id_, geom_) => geom.within(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.Contains) {
      candidates.filter { case (id_, geom_) => geom.contains(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.WithinD) {
      candidates.filter { case (id_, geom_) => geom.isWithinDistance(geom_.asInstanceOf[Geometry], radius) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.Intersects) {
      candidates.filter { case (id_, geom_) => geom.intersects(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.Overlaps) {
      candidates.filter { case (id_, geom_) => geom.overlaps(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.NearestD) {
      //if (candidates.isEmpty)
      //  return Array.empty[(Long, Long)]
      //val nearestItem = candidates.map {
      //  case (id_, geom_) => (id_.asInstanceOf[Long], geom_.asInstanceOf[Geometry].distance(geom))
      //}.reduce((a, b) => if (a._2 < b._2) a else b)
      class dist extends ItemDistance {
        override def distance(itemBoundable: ItemBoundable, itemBoundable1: ItemBoundable): Double = {
          val geom = itemBoundable.getItem.asInstanceOf[(Long, Geometry)]._2
          val geom1 = itemBoundable1.getItem.asInstanceOf[(Long, Geometry)]._2
          geom.distance(geom1)
        }
      }
      val nearestItem = rtree.value.nearestNeighbour(queryEnv, (0l, geom), new dist)
                             .asInstanceOf[(Long, Geometry)]
      Array((leftId, nearestItem._1))
    } else {
      Array.empty[(Long, Long)]
    }
  }

  def apply(sc: SparkContext,
            leftGeometryWithId: RDD[(Long, Geometry)],
            rightGeometryWithId: RDD[(Long, Geometry)],
            joinPredicate: SpatialOperator.SpatialOperator,
            radius: Double = 0): RDD[(Long, Long)] = {
    // create R-tree on right dataset
    val strtree = new STRtree()
    val rightGeometryWithIdLocal = rightGeometryWithId.collect()
    rightGeometryWithIdLocal.foreach(x => {
      val y = x._2.getEnvelopeInternal
      y.expandBy(radius)
      strtree.insert(y, x)
    })
    val rtreeBroadcast = sc.broadcast(strtree)
    leftGeometryWithId.flatMap(x => queryRtree(rtreeBroadcast, x._1, x._2, joinPredicate, radius))
  }
}
