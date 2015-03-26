import Resolvers._
import Dependencies._

lazy val buildSettings = Seq(
  homepage := Some(url("http://dias.epfl.ch/")),
  organization := "raw",
  organizationName := "DIAS/EPFL",
  organizationHomepage := Some(url("http://dias.epfl.ch/")),
  version := "0.0.0",
  scalaVersion := "2.11.6",
  scalacOptions ++= Seq ("-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off")
)

lazy val commonDeps = Seq(
  scalaCompiler,
  scalaReflect,
  scalatest % Test,
  scalacheck,
  scalaLogging,
  logbackClassic
)

lazy val coreDeps = 
  commonDeps ++
  Seq(
    kiama
  )

lazy val rawDeps = 
  commonDeps ++
  Seq(
    shapeless,
    spark,
    json4s    
  )

lazy val raw = (project in file(".")).
  dependsOn(core).
  settings(buildSettings: _*).
  settings(
    resolvers := sonatypeResolvers,
    libraryDependencies ++= rawDeps
  )

lazy val core = (project in file("core")).
  settings(buildSettings: _*).
  settings(
    resolvers := sonatypeResolvers,
    libraryDependencies ++= coreDeps
  )

// Use cached resolution of dependencies (Experimental in SBT 0.13.7)
// http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
updateOptions := updateOptions.value.withCachedResolution(true)
