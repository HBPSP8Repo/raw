package raw.datasets.publications

import org.apache.spark.SparkContext
import raw.executionserver.{AccessPath, Dataset, DefaultSparkConfiguration, JsonLoader}

import scala.reflect._

case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])

case class Author(name: String, title: String, year: Int)

class PublicationsDataset(sc: SparkContext) extends Dataset {
  val authors: List[Author] = JsonLoader.load[Author]("data/publications/authors.json")
  val publications: List[Publication] = JsonLoader.load[Publication]("data/publications/publications.json")

  val authorsRDD = DefaultSparkConfiguration.newRDDFromJSON[Author](authors, sc)
  val publicationsRDD = DefaultSparkConfiguration.newRDDFromJSON[Publication](publications, sc)

  val accessPaths: List[AccessPath[_]] =
    List(
      AccessPath("authors", authorsRDD, classTag[Author]),
      AccessPath("publications", publicationsRDD, classTag[Publication])
    )
}
