package spatialspark.join

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext

import org.apache.spark.rdd.RDD

import spatialspark.operator.SpatialOperator

class BroadcastSpatialJoinTest extends
  FlatSpec with Matchers with SharedSparkContext {

  import spatialspark.join.BroadcastSpatialJoinTest._

  private def BSJ(
    left: RDD[DataType],
    right: RDD[DataType],
    op: SpatialOperator.SpatialOperator,
    radius: Double = 0d,
    condition: Option[ConditionType] = None
  ): Seq[ResultNoGeomType] = {

    BroadcastSpatialJoin(sc, left, right, op, radius, condition)
      .map{ case (ls, rs, _, _) => (ls, rs) }
      .collect
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "contains"
  it should "find polygon that contains point" in {
    // input data
    val leftTab =
      """
        |a:  1,1; 2,1; 2,2
        |aa: 1,1; 2,1; 2,2
        |b:  1,1; 2,2; 1,2
        |bb: 1,1; 2,2; 1,2
      """.stripMargin

    val rightTab =
      """
        |A: 1.5, 1.3
        |B: 1.3, 1.5
      """.stripMargin

    val left: RDD[DataType] = sc.parallelize(parsePolygons(leftTab))
    val right: RDD[DataType] = sc.parallelize(parsePoints(rightTab))

    // join params
    val op = SpatialOperator.Contains
    val radius = 0d // 120 * 1000d // 120 km
    val condition: Option[ConditionType] = Some(
      (ls: String, rs: String) => ls.toLowerCase == rs.toLowerCase
    )

    // expected
    val expected =
      """
        |a, A
        |b, B
      """.stripMargin

    // test
    BSJ(left, right, op, radius, condition) should contain theSameElementsAs parsePairs(expected)

    def noExtraCondition() = {
      val expected =
        """
          |a,  A
          |aa, A
          |b,  B
          |bb, B
        """.stripMargin

      BSJ(left, right, op) should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "within"
  it should "find point within polygon" in {
    // input data
    val left = sc.parallelize(parsePoints(
      """
        |A: 1.5, 1.3
        |B: 1.3, 1.5
      """.stripMargin))

    val right = sc.parallelize(parsePolygons(
      """
        |a:  1,1; 2,1; 2,2
        |aa: 1,1; 2,1; 2,2
        |b:  1,1; 2,2; 1,2
        |bb: 1,1; 2,2; 1,2
      """.stripMargin))

    // join params
    val op = SpatialOperator.Within
    val radius = 0d
    val condition: Option[ConditionType] = Some(
      (ls: String, rs: String) => ls.toLowerCase == rs.toLowerCase
    )

    // expected
    val expected =
      """
        |A, a
        |B, b
      """.stripMargin

    // test
    BSJ(left, right, op, radius, condition) should contain theSameElementsAs parsePairs(expected)

    def noExtraCondition() = {
      val expected =
        """
          |A, a
          |A, aa
          |B, b
          |B, bb
        """.stripMargin

      BSJ(left, right, op) should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "within distance"
  it should "find point within distance from poi" in {
    // input data
    val left = sc.parallelize(parsePoints(
      """
        |A: 1, 1
        |B: 2, 2
      """.stripMargin))

    val right = sc.parallelize(parsePoints(
      """
        |a:  1.1, 1.1   : 16 km to A
        |aa: 1.2, 1.2   : 31 km to A
        |b:  2.1, 2.1   : 16 km to B
        |bb: 2.2, 2.2   : 31 km to B
      """.stripMargin))

    // join params
    val op = SpatialOperator.WithinD
    val radius = 31.5 * 1000d // 31.5 km
    val condition: Option[ConditionType] = Some(
      (ls: String, rs: String) => ls.toLowerCase == rs.toLowerCase
    )

    // expected
    val expected =
      """
        |A, a
        |B, b
      """.stripMargin

    // test
    BSJ(left, right, op, radius, condition) should contain theSameElementsAs parsePairs(expected)

    def noExtraCondition() = {
      val expected =
        """
          |A, a
          |A, aa
          |B, b
          |B, bb
        """.stripMargin

      BSJ(left, right, op, radius) should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "intersects"
  it should "find intersects" in {
    // input data
    val left = sc.parallelize(parsePolylines(
      """
        |A: 1,1; 2,2
        |B: 2,2; 3,3
      """.stripMargin))

    val right = sc.parallelize(parsePolylines(
      """
        |a:  1,2; 2,1
        |aa: 1,2; 2,1
        |b:  2,3; 3,2
        |bb: 2,3; 3,2
      """.stripMargin))

    // join params
    val op = SpatialOperator.Intersects
    val radius = 0d
    val condition: Option[ConditionType] = Some(
      (ls: String, rs: String) => ls.toLowerCase == rs.toLowerCase
    )

    // expected
    val expected =
      """
        |A, a
        |B, b
      """.stripMargin

    // test
    BSJ(left, right, op, radius, condition) should contain theSameElementsAs parsePairs(expected)

    def noExtraCondition() = {
      val expected =
        """
          |A, a
          |A, aa
          |B, b
          |B, bb
        """.stripMargin

      BSJ(left, right, op, radius) should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "overlaps"
  it should "find overlaps" in {
    // input data (of equal dimentions)
    val left = sc.parallelize(parsePolygons(
      """
        |A:  2,4;  2,2;  4,2
        |B: 12,4; 12,2; 14,2
      """.stripMargin))

    val right = sc.parallelize(parsePolygons(
      """
        |a:   3,1;  3,3;  1,3
        |aa:  3,1;  3,3;  1,3
        |b:  13,1; 13,3; 11,3
        |bb: 13,1; 13,3; 11,3
      """.stripMargin))

    // join params
    val op = SpatialOperator.Overlaps
    val radius = 0d
    val condition: Option[ConditionType] = Some(
      (ls: String, rs: String) => ls.toLowerCase == rs.toLowerCase
    )

    // expected
    val expected =
      """
        |A, a
        |B, b
      """.stripMargin

    // test
    BSJ(left, right, op, radius, condition) should contain theSameElementsAs parsePairs(expected)

    def noExtraCondition() = {
      val expected =
        """
          |A, a
          |A, aa
          |B, b
          |B, bb
        """.stripMargin

      BSJ(left, right, op, radius) should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "nearest"
  it should "find nearest points" in {
    // input data
    val left = sc.parallelize(parsePoints(
      """
        |A:   1, 1
        |B:  10, 10
      """.stripMargin))

    val right = sc.parallelize(parsePoints(
      """
        |a:   2, 2
        |aa:  2, 1.9
        |b:  11, 11
        |bb: 11, 10.9
      """.stripMargin))

    // join params
    val op = SpatialOperator.NearestD
    val radius = 0d
    val condition: Option[ConditionType] = Some(
      (ls: String, rs: String) => ls.toLowerCase == rs.toLowerCase
    )

    // expected
    val expected =
      """
        |A, a
        |B, b
      """.stripMargin

    // test
    BSJ(left, right, op, radius, condition) should contain theSameElementsAs parsePairs(expected)

    def noExtraCondition() = {
      val expected =
        """
          |A, aa
          |B, bb
        """.stripMargin

      BSJ(left, right, op, radius) should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "different types"
  it should "check condition with different types" in {
    // input data
    val left = sc.parallelize(parsePoints(
      """
        |A:   1, 1
        |B:  10, 10
      """.stripMargin))

    val right = sc.parallelize(parsePoints(
      """
        |65:   2, 2
        |5 :   2, 1.9
        |66:  11, 11
        |6 :  11, 10.9
      """.stripMargin)).map { case (k, g) => (k.toInt, g)}

    // join params
    val op = SpatialOperator.NearestD
    val radius = 0d
    val condition = Some(
      (lo: String, ro: Int) => lo.head.toInt == ro
    )

    // expected
    val expected =
      """
        |A, 65
        |B, 66
      """.stripMargin

    // test
    BroadcastSpatialJoin(sc, left, right, op, radius, condition)
      .map{ case (ls, rs, _, _) => (ls, rs.toString) }
      .collect should contain theSameElementsAs parsePairs(expected)
  }

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "high latitude"
  it should "find point within distance on high latitude" in {
    // input data
    val left = sc.parallelize(parsePoints(
      """
        |A: 70, 70
        |B: 71, 71
      """.stripMargin))

    val right = sc.parallelize(parsePoints(
      """
        |a:  70.1, 70.1   : 12 km to A
        |aa: 70.2, 70.2   : 24 km to A
        |b:  71.1, 71.1   : 12 km to B
        |bb: 71.2, 71.2   : 23 km to B
      """.stripMargin))

    //def check(left: RDD[DataType], right: RDD[DataType]): Unit = for {
    //  (lKey, lGeom) <- left.collect
    //  (rKey, rGeom) <- right.collect
    //} println(s"$lKey -> $rKey distance: ${math.round(distance(lGeom, rGeom) / 1000d)} km")
    //check(left, right)

    // join params
    val op = SpatialOperator.WithinD
    val condition: Option[ConditionType] = None

    // you definetely want a post-join filter for geometries on high latitude!
    // if you set radius to 12 km, you will get `aa` and `bb` points
    val radius = 6 * 1000d // 6 km

    // test
    val expected =
      """
        |A, a
        |B, b
      """.stripMargin

    BSJ(left, right, op, radius) should contain theSameElementsAs parsePairs(expected)
  }

}

object BroadcastSpatialJoinTest {
  import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
  import util.StringToolbox._
  import scala.language.implicitConversions

  type DataType = (String, Geometry)
  type ConditionType = (String, String) => Boolean
  type ResultNoGeomType = (String, String)

  /**
    * Parse lines like `"a:  1,1; 2,1; 2,2"` into (str, polyline) pairs
    */
  def parsePolylines(str: String): Seq[DataType] = {
    // one line sample: `a:  1,1; 2,1; 2,2`
    val seq: Seq[(String, Geometry)] = for {
      Array(key, points) <- pairs(str)
    } yield (key, polyline(points.splitTrim(";")))
    //println(s"parsePolyline ${seq.head._2.getDimension}: \n\t${seq.mkString("\n\t")}")
    seq
  }

  /**
    * Parse lines like `"a:  1,1; 2,1; 2,2"` into (str, polygon) pairs
    */
  def parsePolygons(str: String): Seq[DataType] = {
    // one line sample: `a:  1,1; 2,1; 2,2`
    val seq: Seq[(String, Geometry)] = for {
      Array(key, points) <- pairs(str)
    } yield (key, polygon(points.splitTrim(";")))
    //println(s"parsePolygon ${seq.head._2.getDimension}: \n\t${seq.mkString("\n\t")}")
    seq
  }

  /**
    * Split `str` by `"\n"`, each line split by `":"`, take first two items;
    * second item split by `","` and make a Point geometry from (x,y) pair
    */
  def parsePoints(str: String): Seq[DataType] = {
    // one line sample: `A: 1.5, 1.3`
    val seq: Seq[(String, Geometry)] = {
      for {
        Array(key, xy) <- pairs(str)
      } yield (key, point(xy.splitTrim(",")))
    }
    //println(s"parsePoints: \n\t${seq.mkString("\n\t")}")
    seq
  }

  /**
    * Split `str` by `"\n"`, each line split by `","` and take first 2 items
    */
  def parsePairs(str: String): Seq[ResultNoGeomType] = {
    // one line sample: `a, A`
    val seq: Seq[(String, String)] = for {
      Array(a, b) <- pairs(str, ",")
    } yield (a, b)
    //println(s"parsePairs: \n\t${seq.mkString("\n\t")}")
    seq
  }

  private val sridWGS84 = 4326
  private val gf = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), sridWGS84)

  implicit private def stringToSeparator(sep: String): Separators = Separators(sep)

  private def polyline(points: Array[String]): Geometry = {
    val coords = for {
      Array(lon, lat) <- points.map(_.splitTrim(","))
    } yield new Coordinate(lon.toDouble, lat.toDouble)

    gf.createLineString(coords)
  }

  private def polygon(points: Array[String]): Geometry = {
    val xys = for {
      Array(lon, lat) <- points.map(_.splitTrim(","))
    } yield (lon, lat)

    val coords: Array[Coordinate] = (xys :+ xys.head) map { case (x, y) =>
      new Coordinate(x.toDouble, y.toDouble) }

    gf.createPolygon(coords)
  }

  private def point(xy: Array[String]): Geometry = xy match {
    case Array(lon, lat) => gf.createPoint(new Coordinate(lon.toDouble, lat.toDouble))
    case _ => throw new IllegalArgumentException(s"xy must be Array(x, y); got `$xy` instead")
  }

  private def pairs(str: String, sep: String = ":") = str.splitTrim("\n")
    .map(_.splitTrim(sep).take(2))

  private def checkContains(left: RDD[DataType], right: RDD[DataType]): Unit = for {
    (areaKey, areaGeom) <- left.collect
    (pointKey, pointGeom) <- right.collect
  } if (areaGeom.contains(pointGeom)) println(s"area `$areaKey` contain point `$pointKey`")

  private def checkDistance(left: RDD[DataType], right: RDD[DataType]): Unit = for {
    (lKey, lGeom) <- left.collect
    (rKey, rGeom) <- right.collect
  } println(s"$lKey -> $rKey distance: ${math.round(distance(lGeom, rGeom) / 1000d)} km")

  /**
    * Geodetic distance between centroids
    */
  def distance(a: Geometry, b: Geometry): Double = {
    import net.sf.geographiclib.Geodesic
    val (p1, p2) = (a.getCentroid, b.getCentroid)
    Geodesic.WGS84.Inverse(p1.getY, p1.getX, p2.getY, p2.getX)
      .s12
  }
}
