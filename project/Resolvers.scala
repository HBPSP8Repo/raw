import sbt._
import Keys._

object Resolvers {
  val sonatypeReleases = Resolver.sonatypeRepo("releases")
  val sonatypeSnapshots = Resolver.sonatypeRepo("snapshots")

  val sonatypeResolvers = Seq(sonatypeReleases, sonatypeSnapshots)
}