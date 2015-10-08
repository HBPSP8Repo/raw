import sbt._

object Dependencies {
  val sparkVersion = "1.4.1"

  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.7"
  val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.11.7"

  val scalatest = "org.scalatest" %% "scalatest" % "2.2.4"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.12.2"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.1.3"
  val log4jOverSlf4j = "org.slf4j" % "log4j-over-slf4j" % "1.7.12"

  // Kiama is treated as a SBT unmanaged dependency (the jar is in core/lib directory)
  //  val kiama = "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" changing()
  // The Kiama dependencies are handled as managed dependencies.
  val kiamaDependencies = Seq(
    "org.bitbucket.inkytonik.dsprofile" % "dsprofile_2.11" % "0.4.0",
    "org.bitbucket.inkytonik.dsinfo" % "dsinfo_2.11" % "0.4.0")

  val spark = "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes") withSources() withJavadoc()
  val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion exclude("org.slf4j", "slf4j-log4j12") withSources() withJavadoc()
  //  val spark = "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes") withSources() withJavadoc()
  //  val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") withSources() withJavadoc()

  val commonsMath = "org.apache.commons" % "commons-math3" % "3.5"
  val commonsIO = "commons-io" % "commons-io" % "2.4"

  val guava = "com.google.guava" % "guava" % "18.0"

  val parserCombinators = "org.scala-lang" % "scala-parser-combinators" % "2.11.0-M4"
  val jackson = "com.fasterxml.jackson.core" % "jackson-core" % "2.6.1"
  val jacksonScala = "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.1"
  val jacksonCsv = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.5.3"

  val metrics = "nl.grons" %% "metrics-scala" % "3.5.2_a2.3"

  val sprayDeps = Seq(
    "io.spray" %% "spray-can" % "1.3.3",
    "com.typesafe.akka" %% "akka-actor" % "2.3.12",
    "com.typesafe.akka" %% "akka-slf4j" % "2.3.12")

  val httpClient = "org.apache.httpcomponents" % "httpclient" % "4.5"

  val scallop = "org.rogach" %% "scallop" % "0.9.5"
  val multisets = "io.github.nicolasstucki" % "multisets_2.11" % "0.3"

  val dropboxSDK = "com.dropbox.core" % "dropbox-core-sdk" % "1.8.1"
  val awsSDK = "com.amazonaws" % "aws-java-sdk" % "1.10.22"
}
