package raw.utils

import java.net.URL
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util
import java.util.function.BiPredicate

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw.RawQuery
import raw.executor.RawScanner

import scala.collection.JavaConversions

object RawUtils extends StrictLogging {

  def cleanOrCreateDirectory(p: Path) = {
    if (Files.isDirectory(p)) {
      logger.info(s"Directory already exists. Deleting contents: $p.")
      FileUtils.cleanDirectory(p.toFile)
    } else {
      logger.info(s"Creating directory: $p")
      Files.createDirectories(p)
    }
  }

  def getTemporaryDirectory(): Path = {
    Paths.get(System.getProperty("java.io.tmpdir"))
  }

  def getTemporaryDirectory(dirname: String): Path = {
    Paths.get(System.getProperty("java.io.tmpdir"), dirname)
  }

  def convertToList(any: Any): Any = {
    try {
      val iterable = any.asInstanceOf[Iterable[_]]
      iterable.toList
    } catch {
      case ex:ClassCastException => any
    }
  }

  def createDirectory(p: Path) = {
    if (Files.isDirectory(p)) {
      logger.info("Directory already exists. Contents: " + util.Arrays.toString(Files.list(p).toArray))
    } else {
      logger.info("Creating directory tree: " + p)
      Files.createDirectory(p)
    }
  }

  def listSubdirectories(basePath: Path): Iterator[Path] = {
    val directoryFilter = new BiPredicate[Path, BasicFileAttributes] {
      override def test(t: Path, u: BasicFileAttributes): Boolean = {
        u.isDirectory && t != basePath
      }
    }
    JavaConversions.asScalaIterator(Files.find(basePath, 1, directoryFilter).iterator())
  }

  def toPath(resource: String): Path = Paths.get(Resources.getResource(resource).toURI)

  /**
   * Extracts a resource from the classpath into the temporary directory.
   *
   * This is needed for instance for loading the macro paradise plugin by the scala compiler created at runtime.
   * The Scala compiler expects to find the jar with the plugin directly in the file system. We are storing this plugin
   * in the resource directory of the project. When the executor is packaged by the sbt-native-plugin, all the resources
   * are placed inside the executor jar, including the macro paradise plugin. So at runtime, the paradise plugin jar must
   * first be extracted from the jar before instantiating the compiler.
   *
   * @param resourceName
   * @return The location of the extracted file in the temporary directory.
   */
  def extractResource(resourceName: String): Path = {
    logger.info("Extracting resource to temporary directory: " + resourceName)
    val mpPlugin: URL = Resources.getResource(resourceName)
    val p = RawUtils.getTemporaryDirectory().resolve(resourceName)
    val is = mpPlugin.openStream()
    try {
      Files.copy(mpPlugin.openStream(), p, StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case ex: FileAlreadyExistsException => // Ignore
    } finally {
      is.close()
    }
    p
  }
}

