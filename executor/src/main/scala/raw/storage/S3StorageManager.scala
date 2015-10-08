package raw.storage

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.util

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.util.IOUtils
import com.typesafe.scalalogging.StrictLogging
import raw.executor.{CodeGenerator, RawSchema, SchemaProperties}

import scala.collection.JavaConversions

class RawS3Object(val key: String, s3: AmazonS3Client) extends RawResource {
  override def openInputStream(): InputStream = {
    val obj: S3Object = s3.getObject(S3StorageManager.bucket, key)
    obj.getObjectContent
  }

  val fileName: String = {
    val idx = key.lastIndexOf("/")
    key.substring(idx + 1)
  }

  override def toString = s"RawS3Object[${key}]"
}

object S3StorageManager {
  final val bucket = "raw-labs-executor"

  case class ScalaObjectListing(delegate: ObjectListing, summaries: List[S3ObjectSummary], path: String, commonPrefixes: List[String], directories: List[String], files: List[String])

  private[this] def toScala[T](col: util.Collection[T]): List[T] = {
    JavaConversions.collectionAsScalaIterable(col).toList
  }

  object ScalaObjectListing {
    def apply(delegate: ObjectListing) = {
      val summaries: List[S3ObjectSummary] = toScala(delegate.getObjectSummaries)
      val path: String = if (delegate.getPrefix == null) "" else delegate.getPrefix
      val commonPrefixes: List[String] = toScala(delegate.getCommonPrefixes)
      val directories: List[String] = commonPrefixes.map(s => s.substring(path.length, s.length - 1))
      val files: List[String] = summaries.map(s => s.getKey.substring(delegate.getPrefix.length))
      new ScalaObjectListing(delegate, summaries, path, commonPrefixes, directories, files)
    }
  }

}

/* TODO: Currently we are caching in memory the schemas read from S3 at startup. This will not work if there are
 * multiple RawServer instances, so we need to either disabled caching completely and always access the schema information
 * from S3 or implement some mechanism to refresh the cache (S3 notifications?)
 */
class S3StorageManager(val stageDirectory: Path) extends StorageManager with StrictLogging {

  import S3StorageManager._

  private[this] val s3 = new AmazonS3Client()
  s3.setRegion(Region.getRegion(Regions.EU_WEST_1))

  // Load schema and properties from local files instead of hitting S3
  override def registerSchema(schemaName: String, stagingDirectory: Path, rawUser: String): Unit = {
    logger.info(s"Registering schema: $schemaName, stageDir: $stagingDirectory, user: $rawUser")

    assert(Files.isDirectory(stagingDirectory))
    val files = JavaConversions.asScalaIterator(Files.list(stagingDirectory).iterator()).toList
    val prefixWithDirName = rawUser + "/" + schemaName
    logger.info(s"Uploading: $prefixWithDirName <- $files")
    // TODO: check if it already exists
    files.foreach(f => uploadFile(bucket, prefixWithDirName, f))
    val schema = loadSchemaFromStorage(rawUser, schemaName)
    val scanner = CodeGenerator.loadScanner(schemaName, schema)
    logger.info("Created scanner: " + scanner)
    scanners.put((rawUser, schemaName), scanner)
  }

  override def listUserSchemasFromStorage(user: String): List[String] = {
    listContents(user + "/").directories
  }

  override def listUsersFromStorage(): List[String] = {
    listContents("").directories
  }

  override def loadSchemaFromStorage(user: String, schemaName: String): RawSchema = {
    val schemaDir = s"$user/$schemaName/"
    logger.info(s"Loading schema: $schemaName at directory: $schemaDir")
    val contents: ScalaObjectListing = listContents(schemaDir)
    logger.info("Files of schema: " + contents.files)

    val list: List[String] = contents.files.filter(s => s.startsWith(schemaName + "."))
    assert(list.size == 1, s"Expected one data file for schema: $schemaName in directory: $schemaDir. Found: $list.")

    val schemaFileName = list.head
    val dataFileKey = schemaDir + schemaFileName
    val propertiesFileKey = schemaDir + "properties.json"
    val schemaFileKey = schemaDir + "schema.xml"
    logger.info(s"Datafile: $dataFileKey, properties: $propertiesFileKey, schemaFile: $schemaFileKey")
    val propertiesString = getObjectAsString(propertiesFileKey)
    val properties = new SchemaProperties(jsonMapper.readValue(propertiesString, classOf[java.util.Map[String, Object]]))
    RawSchema(schemaName, new RawS3Object(schemaFileKey, s3), properties, new RawS3Object(dataFileKey, s3))
  }

  private[this] def listContents(prefix: String): ScalaObjectListing = {
    val req = new ListObjectsRequest()
      .withBucketName(bucket)
      .withPrefix(prefix)
      .withDelimiter("/")
    logger.info("Request: " + req.getPrefix)
    val objs: ScalaObjectListing = ScalaObjectListing(s3.listObjects(req))
    logger.info(s"Prefix: $prefix.\nCommon prefixes: " + objs.commonPrefixes + "\nDirectories: " + objs.directories + "\nFiles: " + objs.files)
    objs
  }

  private[this] def getObjectAsString(key: String): String = {
    val obj: S3Object = s3.getObject(bucket, key)
    val is: S3ObjectInputStream = obj.getObjectContent
    try {
      IOUtils.toString(is)
    } finally {
      obj.close()
    }
  }

  private[this] def putDirectory(prefix: String, directory: Path) = {
    assert(Files.isDirectory(directory))
    val files = JavaConversions.asScalaIterator(Files.list(directory).iterator()).toList
    val prefixWithDirName = prefix + "/" + directory.getFileName
    logger.info(s"Uploading: $prefixWithDirName <- $files")
    files.foreach(f => uploadFile(bucket, prefixWithDirName, f))
  }

  private[this] def uploadFile(bucket: String, prefix: String, p: Path) = {
    val keyName = prefix + "/" + p.getFileName
    logger.info("Uploading: " + keyName + " <- " + p)
    s3.putObject(new PutObjectRequest(bucket, keyName, p.toFile))
  }
}
