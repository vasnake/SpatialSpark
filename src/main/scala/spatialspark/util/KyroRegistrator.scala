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

package spatialspark.util

import com.esotericsoftware.kryo.Kryo
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.strtree.STRtree
import org.apache.spark.serializer.KryoRegistrator

class KyroRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Geometry])
    kryo.register(classOf[STRtree])
    kryo.register(classOf[MBR])
    kryo.register(classOf[String])
  }
}
