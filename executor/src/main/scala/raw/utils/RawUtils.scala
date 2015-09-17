package raw.utils

import java.net.URL
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils

object RawUtils extends StrictLogging {

  def cleanOrCreateDirectory(p: Path) = {
    if (Files.isDirectory(p)) {
      FileUtils.cleanDirectory(p.toFile)
    } else {
      logger.info(s"Creating results directory: $p")
      Files.createDirectory(p)
    }
  }

  def getTemporaryDirectory(): Path = {
    Paths.get(System.getProperty("java.io.tmpdir"))
  }

  def getTemporaryDirectory(dirname: String): Path = {
    Paths.get(System.getProperty("java.io.tmpdir"), dirname)
  }

  def createDirectory(p: Path) = {
    try {
      Files.createDirectory(p)
    } catch {
      case ex: Exception => logger.info("Failed to create directory: " + p + ". Exception: " + ex)
    }
  }

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
      Files.copy(mpPlugin.openStream(), p)
    } catch {
      case ex: FileAlreadyExistsException => // Ignore
    } finally {
      is.close()
    }
    p
  }
}
