logLevel := Level.Warn

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")
