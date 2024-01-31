val versionNumberFile = "VERSION"
val versionNumber = IO.readLines(new File(versionNumberFile))

ThisBuild / version := versionNumber.head

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark-pit",
    idePackagePrefix := Some("io.github.ackuq.pit")
  )

libraryDependencies ++= Seq(
  "com.esotericsoftware.kryo" % "kryo" % "2.24.0",
  "org.slf4j" % "slf4j-api" % "2.0.6",
  "org.scalactic" %% "scalactic" % "3.2.12",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test"
)

// For Databricks 12.2 we need to compile against old java. Java 11 works
// `sbt -java-home /usr/lib/jvm/java-11-openjdk-amd64/ package`
