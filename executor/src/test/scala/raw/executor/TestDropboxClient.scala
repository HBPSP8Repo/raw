package raw.executor

import java.io.IOException
import java.nio.file._

import com.typesafe.scalalogging.StrictLogging
import raw.rest.{ClientErrorException, DropboxClient}

/**
  * Mock version of the dropbox client for use in tests.
  */
trait TestDropboxClient extends DropboxClient {
  /** Returns the token */
  override def getUserName(token: String): String = token


  /** Expects the url to represent a file. Moves it to the destination */
  override def downloadFile(url: String, localFile: Path): Unit = {
    try {
      val srcPath = Paths.get(url)
      Files.copy(srcPath, localFile, StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case ex: IOException => throw new ClientErrorException(ex)
    }
  }
}