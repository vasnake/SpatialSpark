package spatialspark.join

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}

import spatialspark.operator.SpatialOperator
import util.StringToolbox._

class BroadcastSpatialJoinTest extends
  FlatSpec with Matchers with SharedSparkContext {

  import spatialspark.join.BroadcastSpatialJoinTest._

  // testOnly spatialspark.join.BroadcastSpatialJoinTest -- -z "contains"
  it should "area contains points" in {
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
    val op = SpatialOperator.Contains // Within, WithinD, Contains, Intersects, Overlaps, NearestD
    val radius = 0d // 120 * 1000d // 120 km
    val condition: Option[ConditionType] = Some((ls: String, rs: String) =>
      ls.toLowerCase == rs.toLowerCase)

    // expected
    val expectedTab =
      """
        |a, A
        |b, B
      """.stripMargin

    val expected: ResultNoGeomType = parsePairs(expectedTab)

    // test
    val output = BroadcastSpatialJoin(sc, left, right, op, radius, condition)

    output.map{ case (ls, rs, _, _) => (ls, rs) }
      .collect should contain theSameElementsAs expected

    def noExtraCondition() = {
      val expected =
        """
          |a,  A
          |aa, A
          |b,  B
          |bb, B
        """.stripMargin

      BroadcastSpatialJoin(sc, left, right, op, radius, None)
        .map{ case (ls, rs, _, _) => (ls, rs) }
        .collect should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }
}

object BroadcastSpatialJoinTest {

  type DataType = (String, Geometry)
  type ConditionType = (String, String) => Boolean
  type ResultNoGeomType = Seq[(String, String)]

  val sridWGS84 = 4326
  val gf = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), sridWGS84)

  def polygon(points: Array[String]): Geometry = {
    val xys = for {
      Array(lon, lat) <- points.map(_.splitTrim(Separators(",")))
    } yield (lon, lat)

    val coords: Array[Coordinate] = (xys :+ xys.head) map { case (x, y) =>
      new Coordinate(x.toDouble, y.toDouble) }

    gf.createPolygon(coords).asInstanceOf[Geometry]
  }

  def point(xy: Array[String]): Geometry = xy match {
    case Array(lon, lat) => gf.createPoint(new Coordinate(lon.toDouble, lat.toDouble))
    case xz => throw new IllegalArgumentException(s"xy must be Array(x, y); got `$xy` instead")
  }

  def parsePolygons(str: String): Seq[DataType] = {
    // one line sample: `a:  1,1; 2,1; 2,2`
    val seq: Seq[(String, Geometry)] = for {
      Array(key, points) <- str.splitTrim(Separators("\n")).map(_.splitTrim(Separators(":")))
    } yield (key, polygon(points.splitTrim(Separators(";"))))
    //println(s"parsePolygon: \n\t${seq.mkString("\n\t")}")
    seq
  }

  def parsePoints(str: String): Seq[DataType] = {
    // one line sample: `A: 1.5, 1.3`
    val seq: Seq[(String, Geometry)] = {
      for {
        Array(key, xy) <- str.splitTrim(Separators("\n")).map(_.splitTrim(Separators(":")))
      } yield (key, point(xy.splitTrim(Separators(","))))
    }
    //println(s"parsePoints: \n\t${seq.mkString("\n\t")}")
    seq
  }

  def parsePairs(str: String): ResultNoGeomType = {
    // one line sample: `a, A`
    val seq: Seq[(String, String)] = for {
      Array(a, b) <- str.splitTrim(Separators("\n")).map(_.splitTrim(Separators(",")))
    } yield (a, b)
    //println(s"parsePairs: \n\t${seq.mkString("\n\t")}")
    seq
  }

  // check contains
  def check(left: RDD[DataType], right: RDD[DataType]): Unit = for {
    (areaKey, areaGeom) <- left.collect
    (pointKey, pointGeom) <- right.collect
  } if (areaGeom.contains(pointGeom)) println(s"area `$areaKey` contain point `$pointKey`")

}
