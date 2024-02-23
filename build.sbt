ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala212",
    idePackagePrefix := Some("de.fr.sparkscala")
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
// Putting the above together
val sparkVersion = "3.5.0"
val scalaTestVersion = "3.2.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)
