import sbt._
import Keys._

object Dependencies {
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.6"
  val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.11.6"

  val scalatest = "org.scalatest" % "scalatest_2.11" % "2.2.1"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.11.6"
  
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.1.2"

  val kiama = "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" changing()

  val json4s = "org.json4s" %% "json4s-native" % "3.2.10"

  val shapeless = "com.chuusai" %% "shapeless" % "2.1.0"

  val scalaRecords = "ch.epfl.lamp" %% "scala-records" % "0.3"

  val spark = "org.apache.spark" %% "spark-core" % "1.2.0"
}