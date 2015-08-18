package raw.datasets.publications

import org.apache.spark.SparkContext
import raw.datasets.{Dataset, AccessPath}
import raw.executionserver.JsonLoader

import scala.reflect.runtime.universe._

case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])
case class Author(name: String, title: String, year: Int)

object Publications {
  val authorsDS = Dataset("authors", "data/publications/authors.json", typeTag[Author])
  val authorsSmallDS = Dataset("authors", "data/publications/authorsSmall.json", typeTag[Author])

  val publicationsDS = Dataset("publications", "data/publications/publications.json", typeTag[Publication])
  val publicationsLargeDS = Dataset("publications", "data/publications/publicationsLarge.json", typeTag[Publication])
  val publicationsSmallDupsDS = Dataset("publications", "data/publications/publicationsSmallWithDups.json", typeTag[Publication])
  val publicationsSmallDS = Dataset("publications",  "data/publications/publicationsSmall.json", typeTag[Publication])

  object Spark {
    def authorsAP(sc: SparkContext) = AccessPath.toRDDAcessPath(authorsDS, sc)
    def authorsSmallAP(sc: SparkContext) = AccessPath.toRDDAcessPath(authorsSmallDS, sc)
    def publicationsAP(sc: SparkContext) = AccessPath.toRDDAcessPath(publicationsDS, sc)
    def publicationsLargeAP(sc: SparkContext) = AccessPath.toRDDAcessPath(publicationsLargeDS, sc)
    def publicationsSmallDupsAP(sc: SparkContext) = AccessPath.toRDDAcessPath(publicationsSmallDupsDS, sc)
    def publicationsSmallAP(sc: SparkContext) = AccessPath.toRDDAcessPath(publicationsSmallDS, sc)

    def publications(sc: SparkContext) = List(authorsAP(sc), publicationsAP(sc))
    def publicationsLarge(sc: SparkContext) = List(authorsAP(sc),publicationsLargeAP(sc))
    def publicationsSmallDups(sc: SparkContext) = List(authorsSmallAP(sc), publicationsSmallDupsAP(sc))
    def publicationsSmall(sc: SparkContext) = List(authorsSmallAP(sc), publicationsSmallAP(sc))
  }

  object Scala {
    def authorsAP = AccessPath.toScalaAcessPath(authorsDS)
    def authorsSmallAP = AccessPath.toScalaAcessPath(authorsSmallDS)
    def publicationsAP = AccessPath.toScalaAcessPath(publicationsDS)
    def publicationsLargeAP = AccessPath.toScalaAcessPath(publicationsLargeDS)
    def publicationsSmallDupsAP = AccessPath.toScalaAcessPath(publicationsSmallDupsDS)
    def publicationsSmallAP = AccessPath.toScalaAcessPath(publicationsSmallDS)

    def publications() = List(authorsAP, publicationsAP)
    def publicationsLarge() = List(authorsAP,publicationsLargeAP)
    def publicationsSmallDups() = List(authorsSmallAP, publicationsSmallDupsAP)
    def publicationsSmall() = List(authorsSmallAP, publicationsSmallAP)
  }
}
