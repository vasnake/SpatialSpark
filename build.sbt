val Organization = "me.simin"
val Version = "1.1.3-SNAPSHOT"

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
    scalacOptions ++= Seq("-encoding", "UTF-8", "-unchecked", "-deprecation", "-feature"),
    test in assembly := {}
)

// assembly for spark-submit: compile test package assembly
lazy val `spatial-spark` = (project in file("."))
    .settings(
        buildSettings
            ++ Seq(
            libraryDependencies
                ++= geoDeps
                ++ sparkDeps.map(_ % "provided")
                ++ testDeps.map(_ % "test")
        ))
    .settings(
        assemblyOption in assembly := (assemblyOption in assembly).value.copy(
            includeScala = false, includeDependency = true
        ),
        assemblyMergeStrategy in assembly := {
            case n if n.startsWith("META-INF") => MergeStrategy.discard
            case n if n.contains("META-INF/MANIFEST.MF") => MergeStrategy.discard
            case n if n.contains("commons-beanutils") => MergeStrategy.discard
            case n if n.contains("Log$Logger.class") => MergeStrategy.last
            case n if n.contains("Log.class") => MergeStrategy.last
            case x =>
                val oldStrategy = (assemblyMergeStrategy in assembly).value
                oldStrategy(x) // MergeStrategy.deduplicate
        }
    )

// assembly for running as local app: sbt> standalone/assembly
lazy val standalone = project.in(file("standalone"))
    .settings(
        buildSettings ++ Seq(
            libraryDependencies ++= sparkDeps
        ))
    .settings(
        assemblyOption in assembly := (assemblyOption in assembly).value.copy(
            includeScala = true, includeDependency = true
        ),
        assemblyMergeStrategy in assembly := {
            case n if n.startsWith("META-INF") => MergeStrategy.discard
            case n if n.contains("META-INF/MANIFEST.MF") => MergeStrategy.discard
            case n if n.contains("commons-beanutils") => MergeStrategy.discard
            case n if n.contains("Log$Logger.class") => MergeStrategy.last
            case n if n.contains("Log.class") => MergeStrategy.last
            case _ => MergeStrategy.first
        }
    )
    .dependsOn(`spatial-spark`)

// for spark testing
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
parallelExecution in Test := false
fork := true
