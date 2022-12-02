ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

autoScalaLibrary := false

lazy val root = (project in file("."))
  .settings(
    name := "SparkAssignment"
  )

val sparkVersion = "3.0.0-preview2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)

libraryDependencies ++= sparkDependencies
