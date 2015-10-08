package raw

import java.nio.file.Paths

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.storage.S3StorageManager
import raw.utils.RawUtils


class S3StorageTest extends FunSuite with StrictLogging with BeforeAndAfterAll {

  private[this] final val jsonMapper = new ObjectMapper()

  test("awss3") {
    val s3StorageManager = new S3StorageManager(RawUtils.getTemporaryDirectory())
    s3StorageManager.loadFromStorage()
    //    logger.info("Users: " + listUsers())
    //    val buckets = toScala(s3.listBuckets())
    //    logger.info("Objects: " + buckets.mkString("\n"))
    //
    //    val location = s3.getBucketLocation(bucket)
    //    logger.info("Location: " + location)
    //
    //    listObjects()
    //    putDirectory("JaneDane", Paths.get("/home/nuno/rawlabs/testdata/9452650-Nuno_Santos/authors"))
    //    putDirectory("9452650-Nuno_Santos", Paths.get("/home/nuno/rawlabs/testdata/9452650-Nuno_Santos/authors"))
    //    listObjects()

//    s3StorageManager.registerSchema("authors", Paths.get("/home/nuno/rawlabs/testdata/9452650-Nuno_Santos/authors"), "JoeUser")

    val users = s3StorageManager.listUsers()
    logger.info("Users: " + users)
    users.foreach(u => {
      val schemas = s3StorageManager.listUserSchemas(u)
      logger.info(s"User $u schemas: $schemas")
      schemas.foreach(s => {
        val schema = s3StorageManager.loadSchemaFromStorage(u, s)
        logger.info("Schema: " + schema)
      })
    })
  }
}
