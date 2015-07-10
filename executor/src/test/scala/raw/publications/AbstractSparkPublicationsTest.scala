package raw.publications

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.SharedSparkContext
import raw.datasets.publications.{Author, Publication, PublicationsDataset}
import raw.executionserver._

abstract class AbstractSparkPublicationsTest
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with SharedSparkContext
  with ResultConverter {

  var pubsDS: PublicationsDataset = _
  var accessPaths: List[AccessPath[_]] = _
  var authorsRDD: RDD[Author] = _
  var publicationsRDD: RDD[Publication] = _

  override def beforeAll() {
    super.beforeAll()
    pubsDS = new PublicationsDataset(sc)
    authorsRDD = pubsDS.authorsRDD
    publicationsRDD = pubsDS.publicationsRDD
    accessPaths = pubsDS.accessPaths
  }
}
