import Dependencies._
import Resolvers._
import sbt.Keys._

lazy val buildSettings = Seq(
  homepage := Some(url("http://dias.epfl.ch/")),
  organization := "raw",
  organizationName := "DIAS/EPFL",
  organizationHomepage := Some(url("http://dias.epfl.ch/")),
  version := "0.0.0",
  scalaVersion := "2.11.7",
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off"), //"-Ymacro-debug-lite"), //, "-Ymacro-debug-verbose"
  // The flags: "-Ybackend:GenBCode", "-Ydelambdafy:method", "-target:jvm-1.8" enable the generation of Java 8 style
  // lambdas. Spark does not yet work with them.
  // https://github.com/scala/make-release-notes/blob/2.11.x/experimental-backend.md
  //  scalacOptions ++= Seq("-Ybackend:GenBCode", "-Ydelambdafy:method", "-target:jvm-1.8", "-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off"), //"-Ymacro-debug-lite"), //, "-Ymacro-debug-verbose"
  resolvers := sonatypeResolvers,
  // Use cached resolution of dependencies (Experimental in SBT 0.13.7)
  // http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
  updateOptions := updateOptions.value.withCachedResolution(true)
)

lazy val commonDeps = Seq(
  scalatest % Test,
  scalacheck % Test,
  scalaLogging,
  logbackClassic,
  log4jOverSlf4j,
  guava
)

lazy val root = project.in(file("."))
  .aggregate(executor, core)
  .dependsOn(executor)
  .settings(buildSettings)
  .settings(libraryDependencies ++= commonDeps)


lazy val executor = (project in file("executor")).
  dependsOn(core).
  settings(buildSettings ++ addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)).
  settings(
    // The
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)),
    runMain in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)),

    libraryDependencies ++=
      commonDeps ++
        Seq(
          scalaCompiler,
          scalaReflect,
          spark,
          sparkSql,
          jackson,
          jacksonScala,
          httpClient,
          commonMath
        ) ++ sprayDeps,

    // Without forking, Spark SQL fails to load a class using reflection if tests are run from the sbt console.
    // UPDATE: Seems to be working now.
    // Using SBT interactively to run the tests will eventually cause an out of memory error on the metaspace region
    // if tests are run in the same VM.
    fork := true,

    // Alternative to start SBT with -D...=...
    initialize ~= { _ =>
      System.setProperty("raw.compile.server.host", "http://localhost:5000/raw-plan")
    },
    //
    //    testOptions in Test += Tests.Setup(() => {val res = ("bash -c 'python executor/src/test/python/genTests.py'"!)}),
    //    testOptions in Test += Tests.Cleanup(() => println("Cleanup")),
    // only use a single thread for building
    parallelExecution := false,

    // javaOptions is used for forked VMs (fork := true) not when running in process.
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
    },

    /*
      Create a shell script that launches the rest service directly from the shell, thereby bypassing SBT.
      This avoids the overhead of starting SBT (which is considerable) and allows killing the process by simply using
      CTRL+C in the shell (with SBT, CTRL+C kills SBT but leaves the web server running because the server is started
      as a forked process.)
      http://stackoverflow.com/questions/7449312/create-script-with-classpath-from-sbt
      */
    TaskKey[File]("mkrun") <<= (baseDirectory, fullClasspath in Compile, mainClass in Runtime) map { (base, cp, main) =>
      val template = """#!/bin/sh
java -classpath "%s" %s "$@"
"""
      val mainStr = main.getOrElse(sys.error("No main class specified"))
      val contents = template.format(cp.files.absString, mainStr)
      val out = base / "../run-server.sh"
      IO.write(out, contents)
      out.setExecutable(true)
      out
    }
  )

lazy val core = (project in file("core")).
  settings(buildSettings).
  settings(
    libraryDependencies ++= commonDeps ++ Seq(
      kiama)

  )

initialCommands in console := """
                                |import raw.repl._
                                |import raw.repl.RawContext._
                                |val rc = new RawContext()
                                | """.stripMargin
