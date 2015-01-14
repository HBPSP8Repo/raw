name := "raw"

version := "0.0.0"

organization := "raw"

// Scala compiler settings

scalaVersion := "2.11.2"

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
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.11.6" % "test",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2",
    "org.json4s" %% "json4s-native" % "3.2.10",
    "com.googlecode.kiama" %% "kiama" % "1.8.0",
    "com.googlecode.kiama" %% "kiama" % "1.8.0" % "test" classifier ("tests")
  )
  
resolvers ++= Seq (
  Resolver.sonatypeRepo ("releases"),
  Resolver.sonatypeRepo ("snapshots")
)
