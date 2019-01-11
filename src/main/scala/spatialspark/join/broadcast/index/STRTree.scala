/*
 * Copyright 2018 vasnake@gmail.com
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

package spatialspark.join.broadcast.index

import java.util

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.index.strtree.{ItemBoundable, ItemDistance, STRtree}
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._


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
