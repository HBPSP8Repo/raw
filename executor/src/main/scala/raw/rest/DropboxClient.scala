package raw.rest

import java.io.IOException
import java.net.{MalformedURLException, URL}
import java.nio.file.{StandardCopyOption, Files, Path}
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.dropbox.core.{DbxRequestConfig, DbxClient}
import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging

/* Defined as a trait to allow using a mock implementation for tests */
trait DropboxClient {
  def getUserName(token: String): String

  def downloadFile(url: String, localFile: Path): Unit
}

trait RealDropboxClient extends DropboxClient with StrictLogging {

  val config = new DbxRequestConfig("RawLabs S.A.", Locale.getDefault().toString());

  override def getUserName(token: String): String = {
    val client = new DbxClient(config, token)
    val accountInfo = client.getAccountInfo()
    logger.info(s"Client: $accountInfo")
    val displayName = accountInfo.displayName
    // Eliminate spaces, which don't work well when used as part of a directory name.
    accountInfo.userId + "-" + displayName.replace(" ", "_")
  }

  override def downloadFile(url: String, localFile: Path) = {
    logger.info(s"Downloading file: $url")
    try {
      val _url = new URL(url)
      val is = _url.openStream()
      try {
        val start = Stopwatch.createStarted()
        // TODO: Use FileChannel, more efficient.
        val size = Files.copy(is, localFile, StandardCopyOption.REPLACE_EXISTING)
        logger.info(s"Downloaded $size bytes to $localFile in ${start.elapsed(TimeUnit.MILLISECONDS)}ms")
      } finally {
        is.close()
      }
    } catch {
      case ex: MalformedURLException => throw new ClientErrorException(ex)
      case ex: IOException => throw new ClientErrorException(ex)
      case ex: IllegalArgumentException => throw new ClientErrorException(ex)
    }
  }
}
