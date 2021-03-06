import Dependencies._
import Resolvers._
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}
import org.apache.commons.io.FileUtils
import sbt.Keys._


lazy val buildSettings = Seq(
  homepage := Some(url("http://dias.epfl.ch/")),
  organization := "raw",
  organizationName := "DIAS/EPFL",
  organizationHomepage := Some(url("http://dias.epfl.ch/")),
  version := "0.0.0",
  scalaVersion := "2.11.7",
  // Generated Java 8 style lambdas. This reduces significantly the number of classes generated (2700 -> 950)
  // It's an experimental feature so it may not work with some libraries. In particular, it doesn't work with
  // Sparl 1.3.1. Maybe it is fixed in the latest versions of Spark??
  // https://github.com/scala/make-release-notes/blob/2.11.x/experimental-backend.md
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Ydelambdafy:method", "-target:jvm-1.8", "-Ybackend:GenBCode"), // , "-Ypatmat-exhaust-depth", "off"), //"-Ymacro-debug-lite"), //, "-Ymacro-debug-verbose"
//  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
  //  scalacOptions ++= Seq("-Ybackend:GenBCode", "-Ydelambdafy:method", "-target:jvm-1.8", "-deprecation", "-feature", "-unchecked", "-Ypatmat-exhaust-depth", "off"), //"-Ymacro-debug-lite"), //, "-Ymacro-debug-verbose"
  resolvers := sonatypeResolvers,
  // Use cached resolution of dependencies (Experimental in SBT 0.13.7)
  // http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
  updateOptions := updateOptions.value.withCachedResolution(true)
)

lazy val commonDeps = Seq(
  scalatest % Test,
  scalaLogging,
  logbackClassic,
  log4jOverSlf4j,
  guava
)

lazy val root = project.in(file("."))
  .aggregate(executor)
  .dependsOn(executor)
  .settings(buildSettings)
  .settings(libraryDependencies ++= commonDeps)

val startDocker = taskKey[String]("Starts the docker container")
val stopDocker = taskKey[Unit]("Stops the docker container")

lazy val executor = (project in file("executor")).
  //  http://www.scala-sbt.org/sbt-native-packager/formats/docker.html
  enablePlugins(JavaAppPackaging, DockerPlugin).
  settings(
    version in Docker := "latest",
    dockerBaseImage := "nimmis/java:oracle-8-jdk",
    dockerRepository in Docker := Some("rawlabs"),
    dockerExposedPorts in Docker := Seq(54321),
    maintainer in Docker := "Nuno Santos <nuno@raw-labs.com>",
    dockerExposedVolumes in Docker := Seq("/opt/docker/logs"),
    // The plugin by default will run the container as a non-root user and will chown
    // the directory to the give user. We remove the commands so we run as root (inside the container)
    // The chown would also create a new layer in the image containing all the user files again,
    // therefore increasing the final image size by about 150Mb.
    dockerCommands := dockerCommands.value.filterNot {
      // ExecCmd is a case class, and args is a varargs variable, so you need to bind it with @
      case ExecCmd("RUN", args@_*) => args.contains("chown")
      case Cmd("USER", args@_*) => true
      // dont filter the rest
      case cmd => false
    },
    // The inferrer scripts are not part of the scala build, so they have to be added explicitly
    // Override the stage task to also copy the inferrer scripts to the stage directory.
    stage in Docker <<= (baseDirectory, stage in Docker) map { (base, stageDir) =>
      val inferrerDir = new File(base.getParent, "inferrer")
      println(s"Copying inferrer scripts from ${inferrerDir} to $stageDir")
      FileUtils.copyDirectoryToDirectory(inferrerDir, stageDir)
      stageDir
    },
    // Add the RUN command that will install python pip as the second command in the file so docker build reuses the
    // cached layer with this command whenever we rebuild with changes only on our source code.
    dockerCommands := dockerCommands.value.take(2) ++
      Seq(Cmd( """RUN apt-get update && apt-get -y install python-pip python-dev && pip install splitstream && apt-get clean && rm -rf /var/lib/apt/lists/*""")) ++
      dockerCommands.value.drop(2),
    // Update the dockerfile to also copy the inferrer scripts into the image.
    dockerCommands ++= Seq(Cmd("ADD", "inferrer", "/opt/inferrer"))
  ).

  settings(buildSettings ++ addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)).
  settings(resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven").
  settings(
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
          jacksonCsv,
          httpClient,
          scallop,
          metrics,
          multisets,
          dropboxSDK,
          awsSDK,
          juniversalchardet,
          "org.scala-lang.modules" %% "scala-java8-compat" % "0.5.0"
        )
        ++ sprayDeps
        ++ apacheCommonsDeps
        ++ Seq(parserCombinators)
        ++ kiamaDependencies,

    // Without forking, Spark SQL fails to load a class using reflection if tests are run from the sbt console.
    // UPDATE: Seems to be working now.
    // Using SBT interactively to run the tests will eventually cause an out of memory error on the metaspace region
    // if tests are run in the same VM.
    fork := true,

    // Alternative to start SBT with -D...=... Applies to tasks launched within the VM which runs SBT
    //    initialize ~= { _ =>
    //    },

    //        testOptions in Test += Tests.Cleanup(() => println("Cleanup")),
    // only use a single thread for building
    parallelExecution := false,

    // javaOptions is used for forked VMs (fork := true) not when running in process.
    //    javaOptions in run += """-Dspark.master=spark://192.168.1.32:7077""",
    /*
     http://docs.oracle.com/javacomponents/jmc-5-5/jfr-runtime-guide/run.htm
     -XX:FlightRecorderOptions
      delay=XX[smhd]
      duration=XX[smhd]
      dumponexit=true,dumponexitpath=path
     */
    javaOptions ++= Seq( """-Dspark.master=local[2]"""),
    //      """-Draw.inferrer.path=""" + baseDirectory.value + """/../inferrer"""),
    //        """-XX:+UnlockCommercialFeatures""",
    //        """-XX:+FlightRecorder""",
    //        """-XX:StartFlightRecording=delay=5s,settings=rawprofile.jfc,dumponexit=true,filename=myrecording.jfr"""),
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
    mainClass in Runtime := Some("raw.rest.RawRestServerMain"),
    /*
      Create a shell script that launches the rest service directly from the shell, thereby bypassing SBT.
      This avoids the overhead of starting SBT (which is considerable) and allows killing the process by simply using
      CTRL+C in the shell (with SBT, CTRL+C kills SBT but leaves the web server running because the server is started
      as a forked process.)
      http://stackoverflow.com/questions/7449312/create-script-with-classpath-from-sbt
      */
    //    TaskKey[File]("mk-pubs-authors-rest-server") <<= (baseDirectory, fullClasspath in Compile, mainClass in Runtime) map { (base, cp, main) =>

    TaskKey[File]("mk-rest-server") <<= (baseDirectory, fullClasspath in Compile) map { (base, cp) =>
      val template =
        """#!/bin/sh
java -Draw.inferrer.path=%s -classpath "%s" %s "$@"
        """
      val mainStr = "raw.rest.RawRestServerMain"
      val inferrerPath = base + "/../inferrer"
      val contents = template.format(inferrerPath, cp.files.absString, mainStr)
      val out = base / "../bin/run-rest-server.sh"
      IO.write(out, contents)
      out.setExecutable(true)
      out
    }
  )
