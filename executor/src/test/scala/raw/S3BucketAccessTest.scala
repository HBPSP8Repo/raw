package raw

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.scalatest.FunSuite

import scala.collection.JavaConversions

class S3BucketAccessTest extends FunSuite with StrictLogging {

  private[this] val s3client = new AmazonS3Client()
  s3client.setRegion(Region.getRegion(Regions.EU_WEST_1))

  val bucketName = "nfsantos-default"
  test("s3") {
    val objectListing = s3client.listObjects(new ListObjectsRequest().withBucketName(bucketName))
    logger.info("Bucket: " + objectListing)

    val summaries = JavaConversions.iterableAsScalaIterable(objectListing.getObjectSummaries)
    logger.info(s"Found ${summaries.size} keys on bucket ${objectListing.getBucketName}")
    summaries.foreach(s => {
      logger.info("Key: " + s.getKey)
      val is = s3client.getObject(bucketName, s.getKey).getObjectContent
      val contents = IOUtils.toString(is)
      is.close()
      logger.info("Contents: " + contents)
    })
  }
}
