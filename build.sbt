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
      kiama     )

lazy val executorDeps =
  commonDeps ++
    Seq(
      shapeless,
      spark,
      sparkSql,
      json4s
    )

lazy val root = project.in(file("."))
  .aggregate(executor, core)
  .dependsOn(executor)
  .settings(buildSettings)
  .settings(
    libraryDependencies ++= executorDeps
  )

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


initialCommands in console := """
                                |import raw.repl._
                                |import raw.repl.RawContext._
                                |val rc = new RawContext()
                                | """.stripMargin
