/*
 *  Copyright $year Simin You
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */

package spatialspark.exp

import org.locationtech.jts.geom.{Envelope, GeometryFactory}
import org.locationtech.jts.io.WKTReader
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import spatialspark.index.serial.RTree

/**
 * Created by Simin You on 7/27/15.
 */
object RangeQuery {

  val usage = """
    Standalone Implementation of Spatial Range Query on Spark using DataFrame
    Usage: rangequery  --input input
                       --geom geometry index for input data (default 0)
                       --query query window xmin:ymin:xmax:ymax
                       --separator field separator (default TAB)
                       --output output location
                       --use_index
                       --help
              """


  case class ID(id:Long)

  def main(args: Array[String]) {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--help" :: tail =>
          println(usage)
          sys.exit(0)
        case "--input" :: value :: tail =>
          nextOption(map ++ Map('input -> value), tail)
        case "--geom" :: value :: tail =>
          nextOption(map ++ Map('geom -> value.toInt), tail)
        case "--query" :: value :: tail =>
          nextOption(map ++ Map('query -> value), tail)
        case "--separator" :: value :: tail =>
          nextOption(map ++ Map('separator -> value), tail)
        case "--output" :: value :: tail =>
          nextOption(map = map ++ Map('output -> value), list = tail)
        case "--use_index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value.toBoolean), list = tail)
        case "--use_raw" :: value :: tail =>
          nextOption(map = map ++ Map('raw -> value.toBoolean), list = tail)
        case "--build_index" :: value :: tail =>
          nextOption(map = map ++ Map('buildIndex -> value.toBoolean), list = tail)
        case "--index_path" :: value :: tail =>
          nextOption(map = map ++ Map('indexPath -> value), list = tail)

        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)

    val spark = SparkSession.builder().appName("Spatial Query (DataFrame)").getOrCreate()
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    import spark.implicits._


    //.setMaster("local[4]")
      //.setSparkHome("/Users/you/spark-1.4.1")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrator", "me.simin.spark.spatial.util.KyroRegistrator")
    val inputFile = options.getOrElse('input, "").asInstanceOf[String]
    val outputFile = options.getOrElse('output, "").asInstanceOf[String]
    val query = options.getOrElse('query, "").asInstanceOf[String].split(":").map(x => x.toDouble)
    val indexPath = options.getOrElse('indexPath, "").asInstanceOf[String]
    val buildIndex = options.getOrElse('buildIndex, false).asInstanceOf[Boolean]

    val queryMBR = (query(0), query(1), query(2), query(3))

    val geomIdx = options.getOrElse('geom, 0).asInstanceOf[Int]
    val separator = options.getOrElse('separator, "\t").asInstanceOf[String]
    val useIndex = options.getOrElse('index, false).asInstanceOf[Boolean]
    val useRawData = options.getOrElse('raw, false).asInstanceOf[Boolean]

    val timerBegin = System.currentTimeMillis()
    val sc = spark.sparkContext

    val env = new GeometryFactory().toGeometry(new Envelope(queryMBR._1, queryMBR._3, queryMBR._2, queryMBR._4))
    if (useIndex == false) {
      //todo: add non-indexed version
    }
    else {
      val path = indexPath


      def getIndex() = {
        val parquetFile = spark.read.parquet(path)
        parquetFile
      }

      val index = getIndex
      index.createOrReplaceTempView("index")

      def getParquetDF() = {
        try {
          val parquetFile = spark.read.parquet(inputFile)
          parquetFile
        } catch {
          case e: Exception => throw new Exception("failed to load index from " + path + "\n because of " + e.getMessage)
        }
      }

      val timerBegin = System.currentTimeMillis()
      def queryRtree(rtree:Seq[Row], /*query:MBR*/ x0:Double, y0:Double, x1:Double, y1:Double) = {
        val query = (x0, y0, x1, y1)
        val results = RTree.queryRtree(rtree.map(x => (x.getDouble(0), x.getDouble(1), x.getDouble(2), x.getDouble(3),
                                                       x.getLong(4), x.getLong(5))), query, 0)
        results
      }

      spark.udf.register("queryRtree", queryRtree _)

      val candidateDF0 = {
        spark.sql(
          """select queryRtree(index.tree, %f, %f,%f,%f) as ids
            |from index where
            |index.xmax >= %f and
            |index.xmin <= %f and
            |index.ymax >= %f and
            |index.ymin <= %f """.stripMargin
            .format(queryMBR._1, queryMBR._2, queryMBR._3, queryMBR._4,
              queryMBR._1, queryMBR._3, queryMBR._2, queryMBR._4))
      }

      val inputData = getParquetDF
      inputData.createOrReplaceTempView("input")

      val candidateDF = candidateDF0.flatMap { case row => row.getAs[Seq[Long]](0).map(x => ID(x))} .select('id)
      candidateDF.createOrReplaceTempView("candidate")

      val joinResults = inputData.join(candidateDF, inputData("rid") === candidateDF("id"))
      val results2 = joinResults.map(row => (row.getLong(0), row.getString(1))).filter(x => (new WKTReader).read(x._2).intersects(env)).map(x => x._1)
      results2.write.text(outputFile)

      val timerEnd = System.currentTimeMillis()
      println("real query time: " + (timerEnd - timerBegin) + " ms")

    }
    val timerEnd = System.currentTimeMillis()
    println("total time: " + (timerEnd - timerBegin) + " ms")

  }
}
