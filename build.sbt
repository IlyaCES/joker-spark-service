ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark-service"
  )

// Spark
val sparkVersion = "3.2.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
)

// Logging + metrics
libraryDependencies ++= Seq(
  "io.micrometer" % "micrometer-core" % "1.5.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.6",
  "ch.qos.logback" % "logback-classic" % "1.2.6"
)

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test