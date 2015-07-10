import sbt._

object Dependencies {
  val sparkVersion = "1.4.0"

  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.7"
  val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.11.7"

  val scalatest = "org.scalatest" %% "scalatest" % "2.2.4"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.12.3"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.1.3"
  val log4jOverSlf4j = "org.slf4j" % "log4j-over-slf4j" % "1.7.12"

  val kiama = "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" changing()

//  val shapeless = "com.chuusai" %% "shapeless" % "2.1.0"

  // Some libraries imported by Apache Hadoop and Curator have dependencies on old versions of Guava.
  // For the time being, we are not using them, so exclude to avoid compilation warnings.
  val spark = "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes") withSources() withJavadoc()
  val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") withSources() withJavadoc()
//    val spark = "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes")
//    val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion exclude("org.slf4j", "slf4j-log4j12")
//  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.6.0" exclude("org.slf4j", "slf4j-log4j12") exclude("org.slf4j", "log4j-over-slf4j")

  // spark testing is built only for scala 2.10, not compatible with 2.11
  //  val sparkTesting = "com.holdenkarau" % "spark-testing-base_2.10" % "1.3.0_1.1.1_0.0.6"

  val commonMath = "org.apache.commons" % "commons-math3" % "3.5"
  val guava = "com.google.guava" % "guava" % "18.0"

  val jackson = "com.fasterxml.jackson.core" % "jackson-core" % "2.5.2"
  val jacksonScala = "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.5.2"

  val sprayDeps = Seq(
    "io.spray" %% "spray-routing" % "1.3.3",
    "io.spray" %% "spray-can" % "1.3.3",
    "com.typesafe.akka" %% "akka-actor" % "2.3.11",
    "com.typesafe.akka" %% "akka-slf4j" % "2.3.11")

  val httpClient = "org.apache.httpcomponents" % "httpclient" % "4.5"

  val scallop = "org.rogach" %% "scallop" % "0.9.5"
}
