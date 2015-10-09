package raw.rest

import java.net.URL
import java.nio.file.{StandardCopyOption, Files, Path}
import java.util.Locale

import com.dropbox.core.{DbxRequestConfig, DbxClient}
import com.typesafe.scalalogging.StrictLogging

/* Defined as a trait to allow using a mock implementation for tests */
trait DropboxClient {
  def getUserName(token: String): String

  def downloadFile(url: String, localFile: Path): Unit
}

trait RealDropboxClient extends DropboxClient with StrictLogging {

  val config = new DbxRequestConfig("CoolTech S.A.", Locale.getDefault().toString());

  override def getUserName(token: String): String = {
    val client = new DbxClient(config, token)
    val accountInfo = client.getAccountInfo()
    logger.info(s"Client: $accountInfo")
    val displayName = accountInfo.displayName
    // Eliminate spaces, which don't work well when used as part of a directory name.
    accountInfo.userId + "-" + displayName.replace(" ", "_")
  }

  override def downloadFile(url: String, localFile: Path) = {
    // TODO: Use FileChannel, more efficient.
    val is = new URL(url).openStream()
    try {
      val size = Files.copy(is, localFile, StandardCopyOption.REPLACE_EXISTING)
      logger.info(s"Downloaded $size bytes to $localFile")
    } finally {
      is.close()
    }
  }
}
