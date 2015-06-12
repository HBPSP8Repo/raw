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
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off"), //"-Ymacro-debug-lite"), //, "-Ymacro-debug-verbose"
  resolvers := sonatypeResolvers,
  // Use cached resolution of dependencies (Experimental in SBT 0.13.7)
  // http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
  updateOptions := updateOptions.value.withCachedResolution(true),
  addCompilerPlugin(paradiseDependency)
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full

lazy val commonDeps = Seq(
  scalaCompiler,
  scalaReflect,
  scalatest % Test,
  scalacheck % Test,
  scalaLogging,
  logbackClassic,
  log4jOverSlf4j,
  guava
)

lazy val coreDeps =
  commonDeps ++
    Seq(
      kiama)

lazy val executorDeps =
  commonDeps ++
    Seq(
      spark,
      sparkSql,
      jackson,
      jacksonScala,
      commonMath
    )

lazy val root = project.in(file("."))
  .aggregate(executor, core)
  .dependsOn(executor)
  .settings(buildSettings)
  .settings(libraryDependencies ++= executorDeps)

lazy val executor = (project in file("executor")).
  dependsOn(core).
  settings(buildSettings).
  settings(
    // Must use the local spark installation instead of the spark library downloaded by sbt.
    // TODO: must package the local app in a jar and distribute it to the worker nodes using the driver's http server.
    //    unmanagedClasspath in Runtime ++= Seq(file( """c:\Tools\spark-1.3.1-bin-hadoop2.6\lib\spark-assembly-1.3.1-hadoop2.6.0.jar""")).classpath,
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)),

    // Without forking, Spark SQL fails to load a class using reflection if tests are run from the sbt console.
    // UPDATE: Seems to be working now.
    //    fork in Test := true,

    libraryDependencies ++= executorDeps,
    testOptions in Test += Tests.Setup(() => println("Setup")),
    testOptions in Test += Tests.Cleanup(() => println("Cleanup")),

    //    javaOptions in run += """-Dspark.master=spark://192.168.1.32:7077""",
    javaOptions in run += """-Dspark.master=local[2]""",
    // build a JAR with the Spark application plus transitive dependencies.
    // https://github.com/sbt/sbt-assembly
    test in assembly := {}, // Do not run tests when building the assembly
    // Do not include the scala libraries.
    //assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

    // Do not include the Spark unmanaged libraries
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp filter {
        _.data.getName == "spark-assembly-1.3.1-hadoop2.6.0.jar"
      }
    }
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
