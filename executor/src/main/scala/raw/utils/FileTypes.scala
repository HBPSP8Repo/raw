package raw.utils

import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging

object FileTypes extends StrictLogging {
  final val Text = "text"
  final val Json = "json"
  final val Csv = "csv"
  final val KnownFileTypes = List(Text, Json, Csv)

  def inferFileType(filename: Path): String = {
    inferFileType(filename.getFileName.toString)
  }

  def inferFileType(filename: String): String = {
    val i = filename.lastIndexOf('.')
    if (i > 0) {
      val extension = filename.substring(i + 1).toLowerCase
      if (FileTypes.KnownFileTypes.contains(extension)) {
        logger.debug(s"File $filename has a known file extension: $extension.")
        extension
      } else {
        logger.debug(s"File $filename extension is unknown ($extension). Assuming ${FileTypes.Text}")
        FileTypes.Text
      }
    } else {
      logger.debug(s"No extension detected in file $filename. Assuming ${FileTypes.Text}.")
      FileTypes.Text
    }
  }
}

