name := "raw"

version := "0.0.0"

organization := "raw"

organizationName := "DIAS/EPFL"

organizationHomepage := Some(url("http://dias.epfl.ch/"))

homepage := Some(url("http://dias.epfl.ch/"))

// Scala compiler settings

scalaVersion := "2.11.5"

scalacOptions ++= Seq ("-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off")

// Interactive settings

logLevel := Level.Info

shellPrompt <<= (name, version) { (n, v) => _ => n + " " + v + "> " }

// Fork the runs and connect sbt's input and output to the forked process so
// that we are immune to version clashes with the JLine library used by sbt
//
//fork in run := true
//
//connectInput in run := true
//
//outputStrategy in run := Some (StdoutOutput)
//
//// Execution
//
//parallelExecution in Test := false

// Dependencies

resolvers += Opts.resolver.sonatypeReleases
resolvers += Opts.resolver.sonatypeSnapshots

libraryDependencies ++=
  Seq (
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.11.6" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" changing(),
    "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" % "test" classifier ("tests") changing(),
    "ch.qos.logback" % "logback-classic" % "1.1.2",
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.apache.spark" %% "spark-core" % "1.2.0" exclude("org.slf4j", "slf4j-log4j12")
  )

// Use cached resolution of dependencies (Experimental in SBT 0.13.7)
// http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
updateOptions := updateOptions.value.withCachedResolution(true)
