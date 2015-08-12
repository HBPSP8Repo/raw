package raw.datasets.publications

import org.apache.spark.SparkContext
import raw.datasets.AccessPath

case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])
case class Author(name: String, title: String, year: Int)

object Publications {
  def loadAuthors_(sc:SparkContext) =  AccessPath.loadJSON[Author]("authors", "data/publications/authors.json", sc)
  def loadPublications_(sc:SparkContext) =  AccessPath.loadJSON[Publication]("publications", "data/publications/publications.json", sc)
  def loadPublicationsLarge_(sc:SparkContext) =  AccessPath.loadJSON[Publication]("publications", "data/publications/publicationsLarge.json", sc)
  def loadPublicationsSmallDups_(sc:SparkContext) =  AccessPath.loadJSON[Publication]("publications", "data/publications/publicationsSmallWithDups.json", sc)

  def publications(sc: SparkContext) = List(loadAuthors_(sc), loadPublications_(sc))
  def publicationsLarge(sc: SparkContext) = List(loadAuthors_(sc),loadPublicationsLarge_(sc))
  def publicationsSmallDups(sc: SparkContext) = List(loadAuthors_(sc), loadPublicationsSmallDups_(sc))
}