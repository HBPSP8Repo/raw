package raw.datasets.publications

import org.apache.spark.SparkContext
import raw.datasets.Dataset

case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])

case class Author(name: String, title: String, year: Int)


object Publications {
  def loadPublications(sc: SparkContext) = List(
    new Dataset[Author]("authors", "data/publications/authors.json", sc),
    new Dataset[Publication]("publications", "data/publications/publications.json", sc))

  def loadPublicationsLarge(sc: SparkContext) = List(
    new Dataset[Author]("authors", "data/publications/authors.json", sc),
    new Dataset[Publication]("publications", "data/publications/publicationsLarge.json", sc))
}