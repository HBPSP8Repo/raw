import sbt._

object Dependencies {
  val sparkVersion = "1.4.1"

  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.7"
  val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.11.7"

  val scalatest = "org.scalatest" %% "scalatest" % "2.2.4"
  val scalacheck = "org.scalacheck" %% "scalacheck" % "1.12.3"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.1.3"
  val log4jOverSlf4j = "org.slf4j" % "log4j-over-slf4j" % "1.7.12"

  val kiama = "com.googlecode.kiama" %% "kiama" % "2.0.0-SNAPSHOT" changing()

  //  val shapeless = "com.chuusai" %% "shapeless" % "2.1.0"
  val spark = "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes") withSources() withJavadoc()
  val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion exclude("org.slf4j", "slf4j-log4j12") withSources() withJavadoc()
//  val spark = "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes") withSources() withJavadoc()
//  val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") withSources() withJavadoc()

  val commonsMath = "org.apache.commons" % "commons-math3" % "3.5"
  val commonsIO = "commons-io" % "commons-io" % "2.4"

  val guava = "com.google.guava" % "guava" % "18.0"

  val jackson = "com.fasterxml.jackson.core" % "jackson-core" % "2.6.1"
  val jacksonScala = "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.1"

  val sprayDeps = Seq(
    "io.spray" %% "spray-routing" % "1.3.3",
    "io.spray" %% "spray-can" % "1.3.3",
    "io.spray" %% "spray-json" % "1.3.2",
    "com.typesafe.akka" %% "akka-actor" % "2.3.12",
    "com.typesafe.akka" %% "akka-slf4j" % "2.3.12")

  val httpClient = "org.apache.httpcomponents" % "httpclient" % "4.5"

  val scallop = "org.rogach" %% "scallop" % "0.9.5"
  val multisets = "io.github.nicolasstucki" % "multisets_2.11" % "0.3"
}
  // Ignore, left here for reference in case we ever need to use docker-java library
//  val dockerJava = Seq(
//    "com.github.docker-java" % "docker-java" % "1.4.0",
//    "me.lessis" % "odelay-core_2.10" % "0.1.0",
//    "me.lessis" % "undelay_2.10" % "0.1.0"
//  )
  // Spark imports Jersey 1.9, which contains JAXRS 1.X. But docker Java uses a more recent version of Jersey which
  // imports JAXRS 2.0. The 1.9 version should not be on the classpath, otherwise docker-java will fail when it finds
  // the 1.X classes instead of the 2.0 classes. So we exclude the jersey transitive dependency from Spark and explicitly
  // add a more recent version of jersey.
//  val jersey = Seq(
//    "org.glassfish.jersey.core" % "jersey-server" % "2.19")
//  val jaxRS = "javax.ws.rs" % "javax.ws.rs-api" % "2.0.1"
  //  "com.sun.jersey" % "jersey-core" % "1.19",
  //    "com.sun.jersey" % "jersey-server" % "1.19",
  //    "com.sun.jersey" % "jersey-servlet" % "1.19")
  // exclude("javax.ws.rs", "jsr311-api") ws.rs 1.X
//val spark = "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes") excludeAll (
//    ExclusionRule(organization = "com.sun.jersey")
//    ) withSources() withJavadoc()
//  val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion % Provided exclude("org.slf4j", "slf4j-log4j12") excludeAll (
//    ExclusionRule(organization = "com.sun.jersey")
//    ) withSources() withJavadoc()
  //    val spark = "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.curator", "curator-recipes")
  //    val sparkSql = "org.apache.spark" %% "spark-hive" % sparkVersion exclude("org.slf4j", "slf4j-log4j12")
  //  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.6.0" exclude("org.slf4j", "slf4j-log4j12") exclude("org.slf4j", "log4j-over-slf4j")
  // Some libraries imported by Apache Hadoop and Curator have dependencies on old versions of Guava.
  // For the time being, we are not using them, so exclude to avoid compilation warnings.
