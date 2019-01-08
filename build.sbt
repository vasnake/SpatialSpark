val Organization = "me.simin"
val Version = "1.1.2-SNAPSHOT"

// Spark runs on Java 8+, Python 2.7+/3.4+ and R 3.1+. For the Scala API, Spark 2.4.0 uses Scala 2.11
// https://github.com/locationtech/jts/blob/master/MIGRATION.md
// libraryDependencies += "org.locationtech.jts" % "jts" % "1.16.0" pomOnly()

val jts = "com.vividsolutions" % "jts" % "1.13"
val spark = "org.apache.spark" %% "spark-core" % "2.0.2" % "provided"
val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "2.0.2_0.11.0" % "test"

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

assemblyMergeStrategy in assembly := {
  case n if n.startsWith("META-INF") => MergeStrategy.discard
  case n if n.contains("Log$Logger.class") => MergeStrategy.last
  case n if n.contains("Log.class") => MergeStrategy.last
  case n if n.contains("META-INF/MANIFEST.MF") => MergeStrategy.discard
  case n if n.contains("commons-beanutils") => MergeStrategy.discard
  case _ => MergeStrategy.first
}
