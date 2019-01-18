val Organization = "me.simin"
val Version = "1.1.2-SNAPSHOT"

// TODO: Spark 2.4 version
// Spark runs on Java 8+, Python 2.7+/3.4+ and R 3.1+. For the Scala API, Spark 2.4.0 uses Scala 2.11
// https://github.com/locationtech/jts/blob/master/MIGRATION.md
// libraryDependencies += "org.locationtech.jts" % "jts" % "1.16.0" pomOnly()

val sparkVersion = "2.0.2"
val scalaV = "2.11.12"

val geoDeps = Seq("com.vividsolutions" % "jts" % "1.13")

val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDeps = Seq(
    "org.scalatest" %% "scalatest" % "3.0.5",
    "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.11.0"
)

val buildSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := Organization,
  version := Version,
  scalaVersion := scalaV,
  scalacOptions ++= Seq("-encoding", "UTF-8", "-unchecked", "-deprecation", "-feature")
)

lazy val `spatial-spark` = (project in file("."))
    .settings(
        buildSettings
            ++ Seq(
            libraryDependencies
                ++= geoDeps
//                ++ sparkDeps.map(d => d % "provided")
                ++ sparkDeps
                ++ testDeps.map(d => d % "test")
        ))

// for spark testing
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
parallelExecution in Test := false
fork := true

// assembly
assemblyMergeStrategy in assembly := {
    case n if n.startsWith("META-INF") => MergeStrategy.discard
    case n if n.contains("META-INF/MANIFEST.MF") => MergeStrategy.discard
    case n if n.contains("commons-beanutils") => MergeStrategy.discard
    case n if n.contains("Log$Logger.class") => MergeStrategy.last
    case n if n.contains("Log.class") => MergeStrategy.last
    //    case x =>
    //        val oldStrategy = (assemblyMergeStrategy in assembly).value
    //        oldStrategy(x) // MergeStrategy.deduplicate
    case _ => MergeStrategy.first
}

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
    includeScala = true,
    includeDependency = true)
// assemblyPackageDependency + includeDependency=false makes 2 jars
