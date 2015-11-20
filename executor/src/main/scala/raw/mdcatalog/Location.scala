package raw.mdcatalog

import java.io.InputStream
import java.nio.file.{Files, Path}

import raw.storage.S3StorageManager

sealed abstract class Location {
  def openInputStream(): InputStream
}

case class InMemory[T](iterator: Iterable[T]) extends Location {
  override def openInputStream(): InputStream = ???
}

case class LocalFile(path: Path) extends Location {
  override def openInputStream(): InputStream = {
    Files.newInputStream(path)
  }
}

case class S3File(key: String) extends Location {
  override def openInputStream(): InputStream = {
    val schemaS3File = S3StorageManager.s3.getObject(S3StorageManager.bucket, key)
    schemaS3File.getObjectContent
  }
}