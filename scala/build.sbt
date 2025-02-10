val versionNumberFile = "VERSION"
val versionNumber = IO.readLines(new File(versionNumberFile))

ThisBuild / version := versionNumber.head

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark-pit",
    idePackagePrefix := Some("io.github.ackuq.pit")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.0.0-preview1" % "provided",
  "org.apache.spark" %% "spark-sql" % "4.0.0-preview1" % "provided",
  "org.scalactic" %% "scalactic" % "3.2.19",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test"
)
