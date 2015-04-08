import Dependencies._
import Resolvers._
import sbt.Keys._

lazy val buildSettings = Seq(
  homepage := Some(url("http://dias.epfl.ch/")),
  organization := "raw",
  organizationName := "DIAS/EPFL",
  organizationHomepage := Some(url("http://dias.epfl.ch/")),
  version := "0.0.0",
  scalaVersion := "2.11.6",
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off"),
  resolvers := sonatypeResolvers,
// Use cached resolution of dependencies (Experimental in SBT 0.13.7)
// http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
  updateOptions := updateOptions.value.withCachedResolution(true)
)

lazy val commonDeps = Seq(
  scalaCompiler,
  scalaReflect,
  scalatest % Test,
  scalacheck,
  scalaLogging,
  logbackClassic,
  guava
)

lazy val coreDeps =
  commonDeps ++
    Seq(
      kiama
    )

lazy val executorDeps =
  commonDeps ++
    Seq(
      shapeless,
      spark,
      json4s
    )

<<<<<<< HEAD
lazy val root = project.in(file("."))
  .aggregate(executor, core)
  .dependsOn(executor)
  .settings(buildSettings)
  .settings(
    libraryDependencies ++= executorDeps
  )
=======
homepage := Some(url("http://dias.epfl.ch/"))

// Scala compiler settings

scalaVersion := "2.11.6"

// Spark is not compatible with code compiled with jvm-1.8: https://issues.apache.org/jira/browse/SPARK-6152
scalacOptions ++= Seq("-target:jvm-1.7", "-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off")

// Interactive settings

logLevel := Level.Info

shellPrompt <<= (name, version) { (n, v) => _ => n + " " + v + "> " }
>>>>>>> master

lazy val executor = (project in file("executor")).
  dependsOn(core).
  settings(buildSettings).
  settings(
    libraryDependencies ++= executorDeps
  )

lazy val core = (project in file("core")).
  settings(buildSettings).
  settings(
    libraryDependencies ++= coreDeps
  )


<<<<<<< HEAD
initialCommands in console := """
                                |import raw.repl._
                                |import raw.repl.RawContext._
                                |val rc = new RawContext()
                                | """.stripMargin
=======
libraryDependencies ++=
  Seq(
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.11.6" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" changing(),
    "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" % "test" classifier ("tests") changing(),
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.apache.spark" %% "spark-core" % "1.3.0" exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark" %% "spark-sql" % "1.3.0" exclude("org.slf4j", "slf4j-log4j12")
  )

// Use cached resolution of dependencies (Experimental in SBT 0.13.7)
// http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
updateOptions := updateOptions.value.withCachedResolution(true)

initialCommands in console := """
                                |import raw.util._
                                |import raw.util.RawContext._
                                |val rc = new RawContext()
                                | """.stripMargin
>>>>>>> master
