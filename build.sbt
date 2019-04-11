val Organization = "me.simin"
val Version = "1.1.3-SNAPSHOT"

//val sparkVersion = "2.0.2"
//val scalaV = "2.11.12"

val ScalaVersion = "2.12.8"
val SparkVersion = "2.4.1"

val geoDeps = Seq(
  //"com.vividsolutions" % "jts" % "1.13"
  // https://github.com/locationtech/jts/blob/master/MIGRATION.md
  "org.locationtech.jts" % "jts-core" % "1.16.1"
)

val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % SparkVersion,
    "org.apache.spark" %% "spark-sql" % SparkVersion
)

val testDeps = Seq(
    "org.scalatest" %% "scalatest" % "3.0.5",
    "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0"
    //"com.holdenkarau" %% "spark-testing-base" % s"${SparkVersion}_0.11.0"
)

val buildSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := Organization,
    version := Version,
    scalaVersion := ScalaVersion,
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
