/*
 * Copyright 2015 Simin You (2017 Valentin Fedulov)
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
import org.locationtech.jts.index.strtree.{ItemBoundable, ItemDistance, STRtree}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import spatialspark.operator.SpatialOperator


object BroadcastSpatialJoin {

  // Why do we need arbitrary object instead of key[Long]?
  // For arbitrary datasets not always possible select deterministic key[Long].
  // Sometimes, using key[Long] you need to make a second join, to select
  // additional columns for resulting dataset. Using arbitrary object (e.g. Row)
  // it's possible to eliminate secondary join. Also, it's possible to filter data before
  // spatial predicate application, using condition(leftObject, rightObject) => Boolean

  // Why do we need Geometry objects in join result?
  //  You may want to apply more accurate filters after join, based on geometry objects.
  //  You may want to produce some derivative from (leftGeom, rightGeom), e.g. area,
  //  perimeter, center, distance, etc.
  //  You may want to join another dataset with spatial relation,
  //  or extend your pipeline with any spatial functions

  // TODO: add possibility to choose, WGS84 or Eucledian coords.
  // TODO: add join type option, 'inner', 'left-outer' .
  // TODO: add projection operator, possibility to define result columns.
  // TODO: extra condition type: ((L, Geom), (R, Geom)) => Boolean

  /**
    * Inner join: for each record from left find all records in right, that satisfy
    * `condition` and `joinPredicate`.
    * Geometries should be (Lon,Lat) based, in WGS84 model.
    *
    * <p>We have one special case: joinPredicate is `WithinD` :</p>
    *
    *   <p>in this case, after join you may have to apply more accurate filter
    *   using geodetic distance implementation. Join uses JTS distance implementation,
    *   that don't work with Lon,Lat coordinates and for that reason
    *   here we have to apply `metre2degree` conversion for `radius` parameter,
    *   and conversion result depends on geometry latitude.</p>
    *
    * @param sc context
    * @param left big dataset for iterate over (flatmap)
    * @param right small dataset, will be broadcasted
    * @param joinPredicate spatial relation, one of (
    *                      `WithinD`, `Within`, `Contains`, `Intersects`, `Overlaps`, `NearestD`).
    *                      Relation will be queried in form: `leftGeom.relate(rightGeom)`
    * @param radius distance in meters, used in `WithinD` relation:
    *               `leftGeom.isWithinDistance(rightGeom, maxArcLen)`,
    *               where `maxArcLen` is an approximate arc len in degrees (WGS84 model) on a latitude of rightGeom.
    * @param condition function that will be applied to each left row and each candidate to join from right dataset;
    *                  candidate will be joined if `condition` is `true` and geometries satisfy the `joinPredicate`.
    * @tparam L type of left object
    * @tparam R type of right object
    * @return inner join result: (left, right, leftGeom, rightGeom)
    */
  def apply[L, R](sc: SparkContext,
                  left: RDD[(L, Geometry)],
                  right: RDD[(R, Geometry)],
                  joinPredicate: SpatialOperator.SpatialOperator,
                  radius: Double = 0,
                  condition: Option[(L, R) => Boolean] = None
                 ): RDD[(L, R, Geometry, Geometry)] = {

    // create ST-R-tree index on right dataset
    val index = sc.broadcast(buildIndex[R](right, radius))

    // add extra condition to `nearest` metric computations
    val nearestMetric = condition match {
      case None => new ItemDistanceImpl
      case Some(predicate) => new ItemDistanceWithConditionImpl(predicate)
    }

    left
        .filter(r => r._2 != null) // geometry may be null
        .flatMap { case (leftObj, leftGeom) => {
          val rightRecs = queryRtree[L, R](
            index, leftObj, leftGeom, joinPredicate, condition, nearestMetric
          )

          // inner join
          rightRecs.map { case (rightObj, rightGeom) => (leftObj, rightObj, leftGeom, rightGeom) }
          // TODO: left outer join?
        }}
  }

  private
  def queryRtree[L, R](index: Broadcast[STRtree],
                       leftObj: L,
                       leftGeom: Geometry,
                       predicate: SpatialOperator.SpatialOperator,
                       condition: Option[(L, R) => Boolean],
                       nearestMetric: ItemDistance
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

    def nearest = index.value.nearestNeighbour(
      queryBox,
      makeItem(leftObj, leftGeom, 0d),
      nearestMetric
    )

    predicate match {
      // Why do we need variable itemDistance instead of const radius?
      // JTS distance work with Eucledian coords, but we are using Lon,Lat =>
      // distance in degrees different for different latitude for same distance in metres.
      case SpatialOperator.WithinD => result(candidates.filter(f =>
        leftGeom.isWithinDistance(itemGeometry(f), itemDistance(f))))

      case SpatialOperator.Within => filterCandidates(leftGeom.within)
      case SpatialOperator.Contains => filterCandidates(leftGeom.contains) // ?covers?
      case SpatialOperator.Intersects => filterCandidates(leftGeom.intersects)
      case SpatialOperator.Overlaps => filterCandidates(leftGeom.overlaps)

      case SpatialOperator.NearestD => if (index.value.size() > 0) {
        result(Array(nearest))
      } else emptyResult

      // unknown predicate
      case _ => emptyResult
    }
  }

  // Objects stored in index: (T, Geometry, distanceLimit) // TODO: case class?
  private def makeItem[T](obj: T, geom: Geometry, dist: Double) = (obj, geom, dist)
  private def itemObject[T](item: AnyRef) = item.asInstanceOf[(T, _, _)]._1
  private def itemGeometry(item: AnyRef) = item.asInstanceOf[(_, Geometry, _)]._2
  // WithinD predicate implementation details
  private def itemDistance(item: AnyRef) = item.asInstanceOf[(_, _, Double)]._3

    /**
    * Distance(g1, g2) implementation for NearestD predicate processing.
    * JTS Geometry.distance have two main disadvantages:
    *   <li>it's slow O(n*n);</li>
    *   <li>it's Eucledian and don't work with WGS84 Lon,Lat coordinates properly.</li>
    * But, it also have a huge advantage: you don't need to implement
    * fast and accurate distance(g1, g2), because it fast enough for simple geometries
    * and for NearestD predicate we need only relative distance between pairs of objects.
    */
  class ItemDistanceImpl extends
    ItemDistance with Serializable {

    override def distance(item1: ItemBoundable, item2: ItemBoundable): Double =
      geom(item1).distance(geom(item2))

    protected def geom(obj: ItemBoundable): Geometry = itemGeometry(obj.getItem)
  }

  /**
    * Distance(obj1, obj2) implementation for NearestD predicate with extra condition.
    * Extra condition is a function that check objects attributes and make a decision:
    * objects pair is good to consider a distance between them or we must skip this pair.
    * @param predicate extra condition function
    * @tparam L left object type
    * @tparam R right object type
    */
  class ItemDistanceWithConditionImpl[L, R](predicate: (L, R) => Boolean) extends
    ItemDistanceImpl with Serializable {

    override def distance(item1: ItemBoundable, item2: ItemBoundable): Double = {
      // left passed as item2; right (from index) passed as item1
      val left = obj[L](item2)
      val right = obj[R](item1)
      //val p = predicate(left, right)
      //println(s"ItemDistanceWithConditionImpl; left: $left; right: $right; predicate: $p;")

      if (predicate(left, right)) super.distance(item1, item2)
      else Double.MaxValue
    }

    private def obj[T](o: ItemBoundable) = itemObject[T](o.getItem)
  }

  private def buildIndex[T](rdd: RDD[(T, Geometry)], maxDistance: Double) = {
    val strtree = new STRtree()

    rdd
        .filter { case (_, geom) => geom != null }
        .map { case (obj, geom) => {
          // envelope will be expanded if maxDistance != 0; use it with WithinD predicate
          val envelope = geom.getEnvelopeInternal

          // Why do we need variable distance?
          // Because assumption is that we use WGS84 (Lon,Lat) coords, but we use meters
          // for radius/distance metric.
          // JTS distance work with Eucledian coords, but we are using Lon,Lat =>
          // distance in degrees is different for different latitude for same distance in metres.
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

  /**
    * Length of arc on equator about 100km for 1 degree
    */
  private val degreesInMeter = 1d / 111000d
  private val pi180 = math.Pi / 180d

  /**
    * For high latitude returns bigger value: length of arc in degrees along parallel on that latitude
    * @param metre desirable length of arc in meters
    * @param lat scale coefficient effectively
    * @return approximate value for arc length in degrees
    */
  private def metre2degree(metre: Double, lat: Double): Double = {
    (metre * degreesInMeter) / math.cos(lat * pi180)
  }

}
