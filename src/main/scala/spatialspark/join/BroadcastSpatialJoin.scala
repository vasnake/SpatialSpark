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
import com.vividsolutions.jts.index.strtree.{ItemBoundable, ItemDistance, STRtree}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import spatialspark.operator.SpatialOperator
import spatialspark.join.broadcast.index._

object BroadcastSpatialJoin {

  // ((T, Geometry), distanceLimit)
  // TODO: why do we need arbitrary object instead of old Long? Dataset w/o key[Long]?
  private def makeItem[T](obj: T, geom: Geometry, dist: Double) = ((obj, geom), dist)
  private def itemObject[T](item: AnyRef) = item.asInstanceOf[((T, _), _)]._1._1
  private def itemGeometry(item: AnyRef) = item.asInstanceOf[((_, Geometry), _)]._1._2
  // WithinD predicate implementation details
  private def itemDistance(item: AnyRef) = item.asInstanceOf[((_, _), Double)]._2

  // NearestD predicate implementation details
  class STRTreeItemDistanceImpl(geom: ItemBoundable => Geometry)
      extends ItemDistance with Serializable {

    override def distance(item1: ItemBoundable, item2: ItemBoundable): Double = {
      geom(item1).distance(geom(item2))
    }
  }

  private val distanceFunc = new STRTreeItemDistanceImpl(
    item => itemGeometry(item.getItem)
  )

  private
  def queryRtree[L, R](index: Broadcast[STRtree],
                       leftObj: L,
                       leftGeom: Geometry,
                       predicate: SpatialOperator.SpatialOperator,
                       condition: Option[(L, R) => Boolean]
                      ): Array[(R, Geometry)] = {

    val queryBox = leftGeom.getEnvelopeInternal

    lazy val candidates = {
      val features = index.value.query(queryBox).toArray

      condition match {
        case None => features
        case Some(cond) => features.filter(f => cond(leftObj, itemObject[R](f)))
      }
    }

    def result(lst: Array[AnyRef]) = lst.map { f => (itemObject[R](f), itemGeometry(f)) }
    def emptyResult = result(Array.empty)
    def filterCandidates(leftRelRight: Geometry => Boolean) =
      result(candidates.filter(f => leftRelRight(itemGeometry(f))))

    predicate match {
      // TODO: why do we need variable itemDistance instead of const radius?
      case SpatialOperator.WithinD => result(candidates.filter(f =>
        leftGeom.isWithinDistance(itemGeometry(f), itemDistance(f))))

      case SpatialOperator.Within => filterCandidates(leftGeom.within)
      case SpatialOperator.Contains => filterCandidates(leftGeom.contains)
      case SpatialOperator.Intersects => filterCandidates(leftGeom.intersects)
      case SpatialOperator.Overlaps => filterCandidates(leftGeom.overlaps)

      // TODO: extra condition is not applied here; you shouldn't use condition with NearestD predicate!
      case SpatialOperator.NearestD => if (index.value.size() > 0) {
        val item = index.value.nearestNeighbour(queryBox, ((null, leftGeom), null), distanceFunc)
        result(Array(item))
      } else emptyResult

      // unknown predicate
      case _ => emptyResult
    }
  }

  /**
    * For each record in left find all records in right, that satisfy joinPredicate.
    * Geometries should be in Lon,Lat-based.
    * @param sc context
    * @param left big dataset for iterate over (flatmap)
    * @param right small dataset, will be broadcasted
    * @param joinPredicate spatial relation, e.g. contain, intersects, ...
    * @param radius meters, used in withinD relation: leftGeom.isWithinDistance(rightGeom, radius)
    * @tparam L type of left object
    * @tparam R type of right object
    * @return inner join result: (left, right, left_geom, right_geom)
    */
  def apply[L, R](sc: SparkContext,
                  left: RDD[(L, Geometry)],
                  right: RDD[(R, Geometry)],
                  joinPredicate: SpatialOperator.SpatialOperator,
                  radius: Double = 0,
                  condition: Option[(L, R) => Boolean] = None
                 ): RDD[(L, R, Geometry, Geometry)] = {

    // create ST-R-tree on right dataset
    val index = sc.broadcast(buildIndex[R](right, radius))

    left
        .filter(r => r._2 != null)
        .flatMap { case (leftObj, leftGeom) =>
          val rightRecs = queryRtree[L, R](index, leftObj, leftGeom, joinPredicate, condition)
          // inner join
          rightRecs.map { case (rightObj, rightGeom) => (leftObj, rightObj, leftGeom, rightGeom) }
          // TODO: left join
        }
  }

  private def buildIndex[T](rdd: RDD[(T, Geometry)], maxDistance: Double) = {
    val strtree = new STRtree()

    rdd
        .filter { case (_, geom) => geom != null }
        .map { case (obj, geom) => {
          val envelope = geom.getEnvelopeInternal
          // TODO: why do we need variable degree distance?
          val maxDegrees: Double = if (maxDistance != 0) {
            val deg = metre2degree(maxDistance, envelope.centre.y)
            envelope.expandBy(deg)
            deg
          }
          else 0

          (envelope, makeItem(obj, geom, maxDegrees))
        }}
        .collect
        .foreach(rec => strtree.insert(rec._1, rec._2))

    strtree
  }

  // TODO: why divide by cos(latitude)? where from that magic constant?
  private val degreesInMeter = 1d / 111200d
  private val pi180 = math.Pi / 180d

  private def metre2degree(metre: Double, lat: Double): Double = {
    (metre * degreesInMeter) / math.cos(lat * pi180)
  }

}
