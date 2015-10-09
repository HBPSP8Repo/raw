package raw.executor

import java.nio.file.{StandardCopyOption, Files, Paths, Path}

import raw.rest.DropboxClient


trait TestDropboxClient extends DropboxClient {
  /** Returns the token */
  override def getUserName(token: String): String = token

  /** Expects the url to represent a file. Moves it to the destination */
  override def downloadFile(url: String, localFile: Path): Unit = {
    val srcPath = Paths.get(url)
    Files.move(srcPath, localFile, StandardCopyOption.REPLACE_EXISTING)
  }
}