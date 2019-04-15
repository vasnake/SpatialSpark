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

      BSJ(left, right, op, radius) should contain theSameElementsAs parsePairs(expected)
    }

    noExtraCondition()
  }

  // TODO: Within, WithinD, Intersects, Overlaps, NearestD
}

object BroadcastSpatialJoinTest {
  import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
  import util.StringToolbox._
  import scala.language.implicitConversions

  type DataType = (String, Geometry)
  type ConditionType = (String, String) => Boolean
  type ResultNoGeomType = (String, String)

  def parsePolygons(str: String): Seq[DataType] = {
    // one line sample: `a:  1,1; 2,1; 2,2`
    val seq: Seq[(String, Geometry)] = for {
      Array(key, points) <- pairs(str)
    } yield (key, polygon(points.splitTrim(";")))
    //println(s"parsePolygon: \n\t${seq.mkString("\n\t")}")
    seq
  }

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

  def parsePairs(str: String): Seq[ResultNoGeomType] = {
    // one line sample: `a, A`
    val seq: Seq[(String, String)] = for {
      Array(a, b) <- pairs(str, ",")
    } yield (a, b)
    //println(s"parsePairs: \n\t${seq.mkString("\n\t")}")
    seq
  }

  val sridWGS84 = 4326
  val gf = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), sridWGS84)

  implicit private def stringToSeparator(sep: String): Separators = Separators(sep)

  private def polygon(points: Array[String]): Geometry = {
    val xys = for {
      Array(lon, lat) <- points.map(_.splitTrim(","))
    } yield (lon, lat)

    val coords: Array[Coordinate] = (xys :+ xys.head) map { case (x, y) =>
      new Coordinate(x.toDouble, y.toDouble) }

    gf.createPolygon(coords).asInstanceOf[Geometry]
  }

  private def point(xy: Array[String]): Geometry = xy match {
    case Array(lon, lat) => gf.createPoint(new Coordinate(lon.toDouble, lat.toDouble))
    case _ => throw new IllegalArgumentException(s"xy must be Array(x, y); got `$xy` instead")
  }

  private def pairs(str: String, sep: String = ":") = str.splitTrim("\n").map(_.splitTrim(sep))

  // check contains
  def check(left: RDD[DataType], right: RDD[DataType]): Unit = for {
    (areaKey, areaGeom) <- left.collect
    (pointKey, pointGeom) <- right.collect
  } if (areaGeom.contains(pointGeom)) println(s"area `$areaKey` contain point `$pointKey`")

}
