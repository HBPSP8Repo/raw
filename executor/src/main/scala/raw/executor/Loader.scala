package raw.executor

import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging

import scala.reflect._

object Loader extends StrictLogging {

  def loadAbsolute[T](p: Path)(implicit m: Manifest[T]): T = {
    if (p.toString.endsWith(".json")) {
      JsonLoader.loadAbsolute(p)
    } else if (p.toString.endsWith(".csv")) {
      CsvLoader.loadAbsolute(p)
    } else {
      throw new IllegalArgumentException("Unsupported file type: " + p)
    }
  }
}

