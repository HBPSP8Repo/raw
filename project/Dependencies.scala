import sbt._
import Keys._

object Dependencies {
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.6"
  val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.11.6"

  val scalatest = "org.scalatest" % "scalatest_2.11" % "2.2.1"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.11.6"
  
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.1.3"

  val kiama = "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" changing()

  val json4s = "org.json4s" %% "json4s-native" % "3.2.10"

  val shapeless = "com.chuusai" %% "shapeless" % "2.1.0"

  // Some libraries imported by Apache Hadoop and Curator have dependencies on old versions of Guava.
  // For the time being, we are not using them, so exclude to avoid compilation warnings.
  val spark = "org.apache.spark" %% "spark-core" % "1.3.0" exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes") excludeAll(ExclusionRule(organization = "org.apache.hadoop"))

  val guava = "com.google.guava" % "guava" % "18.0"
}
