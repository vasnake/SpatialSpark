package spatialspark.join

import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.Row
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}

import spatialspark.join.broadcast.index.{FeatureExt, RtreeIndex}


class RtreeIndexSpec extends FunSuite with Matchers {

    test("RtreeIndex can be created and queried") {

        //given
        val kremlin = new GeometryFactory().createPolygon(Seq(
            new Coordinate(37.611681d, 55.747691d),
            new Coordinate(37.609922d, 55.755436d),
            new Coordinate(37.622582d, 55.755848d),
            new Coordinate(37.623311d, 55.749071d),
            new Coordinate(37.611681d, 55.747691d)
        ).toArray).getEnvelopeInternal

        val zaryadie = new GeometryFactory().createPolygon(Seq(
            new Coordinate(37.625586d, 55.749979d),
            new Coordinate(37.625521d, 55.752024d),
            new Coordinate(37.632087d, 55.752338d),
            new Coordinate(37.632366d, 55.749930d),
            new Coordinate(37.625586d, 55.749979d)
        ).toArray).getEnvelopeInternal

        val sobornayaPloschad = new GeometryFactory().createPolygon(Seq(
            new Coordinate(37.617083d, 55.750336d),
            new Coordinate(37.616987d, 55.750783d),
            new Coordinate(37.617909d, 55.750853d),
            new Coordinate(37.618054d, 55.750464d),
            new Coordinate(37.617083d, 55.750336d)
        ).toArray).getEnvelopeInternal

        val kremlinFeat = FeatureExt(
            Row.fromSeq(Seq("0_1", "0_2")),
            new GeometryFactory().createPoint(new Coordinate(37.611681d, 55.747691d)),
            0d)

        val features = Seq(
            (kremlin, kremlinFeat)
        )

        //when
        val rtree = RtreeIndex(features)

        //then
        rtree.featuresInEnvelope(zaryadie) shouldBe Seq()
        rtree.featuresInEnvelope(sobornayaPloschad) shouldBe Seq(kremlinFeat)
    }

}
