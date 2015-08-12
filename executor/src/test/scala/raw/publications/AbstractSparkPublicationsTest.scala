package raw.publications

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import raw.AbstractSparkTest
import raw.datasets.AccessPath
import raw.datasets.publications.{Author, Publication}

import scala.reflect.runtime.universe._

abstract class AbstractSparkPublicationsTest(loader: (SparkContext) => List[AccessPath[_ <: Product]])
  extends AbstractSparkTest(loader) {

  var authorsRDD: RDD[Author] = _
  var publicationsRDD: RDD[Publication] = _

  override def beforeAll() {
    super.beforeAll()
    try {
      authorsRDD = accessPaths.filter(ap => ap.tag == typeTag[Author]).head.path.asInstanceOf[RDD[Author]]
      publicationsRDD = accessPaths.filter(ap => ap.tag == typeTag[Publication]).head.path.asInstanceOf[RDD[Publication]]
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
