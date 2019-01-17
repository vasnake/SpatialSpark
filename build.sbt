val Organization = "me.simin"
val Version = "1.1.2-SNAPSHOT"

// Spark runs on Java 8+, Python 2.7+/3.4+ and R 3.1+. For the Scala API, Spark 2.4.0 uses Scala 2.11
// https://github.com/locationtech/jts/blob/master/MIGRATION.md
// libraryDependencies += "org.locationtech.jts" % "jts" % "1.16.0" pomOnly()

val sparkVersion = "2.0.2"

val jts = "com.vividsolutions" % "jts" % "1.13"

//val spark = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
//val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
val spark = "org.apache.spark" %% "spark-core" % sparkVersion
val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion
//val hadoop = "org.apache.hadoop" % "hadoop-common" % "2.7.7"

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.11.0" % "test"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true, includeDependency = true)
// assemblyPackageDependency + includeDependency=false makes 2 jars

val buildSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := Organization,
  version := Version,
  scalaVersion := "2.11.12",
  scalacOptions ++= Seq("-encoding", "UTF-8", "-unchecked", "-deprecation", "-feature"),
  parallelExecution := false
)

lazy val spatialSpark: Project = Project("spatial-spark", file("."))
    .settings(
      buildSettings ++ Seq(
          libraryDependencies ++= Seq(jts, scalaTest, sparkTestingBase, spark, sparkSQL)
      ))
