package raw.rest

import java.net.URL
import java.nio.file.{StandardCopyOption, Files, Path}
import java.util.Locale

import com.dropbox.core.{DbxRequestConfig, DbxClient}
import com.typesafe.scalalogging.StrictLogging

object DropboxClient extends StrictLogging {

  val config = new DbxRequestConfig("JavaTutorial/1.0", Locale.getDefault().toString());

  def getUserName(token: String): String = {
    val client = new DbxClient(config, token)
    val accountInfo = client.getAccountInfo()
    logger.info(s"Client: $accountInfo")
    val displayName = accountInfo.displayName
    accountInfo.userId + "-" + displayName.replace(" ", "_")
  }

  def downloadFile(url: String, localFile: Path) = {
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
