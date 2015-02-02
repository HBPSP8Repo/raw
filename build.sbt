name := "raw"

version := "0.0.0"

organization := "raw"

// Scala compiler settings

scalaVersion := "2.11.5"

scalacOptions ++= Seq ("-deprecation", "-feature", "-unchecked")

// Interactive settings
logLevel := Level.Info

shellPrompt <<= (name, version) { (n, v) => _ => n + " " + v + "> " }

// Fork the runs and connect sbt's input and output to the forked process so
// that we are immune to version clashes with the JLine library used by sbt

fork in run := true

connectInput in run := true

outputStrategy in run := Some (StdoutOutput)

// Execution

parallelExecution in Test := false

// Dependencies

libraryDependencies ++=
  Seq (
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.11.6" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "ch.qos.logback" % "logback-classic" % "1.1.2",
    "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT",
    "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" % "test" classifier ("tests"),
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.apache.spark" %% "spark-core" % "1.2.0"
  )

resolvers ++= Seq (
  Resolver.sonatypeRepo ("releases"),
  Resolver.sonatypeRepo ("snapshots")
)
