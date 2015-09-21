package raw.executor

import com.typesafe.scalalogging.StrictLogging

import scala.reflect._

object Loader extends StrictLogging {

  def loadAbsolute[T](schema: RawSchema)(implicit m: Manifest[T]): T = {
    val p = schema.dataFile
    if (p.toString.endsWith(".json")) {
      JsonLoader.loadAbsolute(schema)
    } else if (p.toString.endsWith(".csv")) {
      CsvLoader.loadAbsolute(schema)
    } else {
      throw new IllegalArgumentException("Unsupported file type: " + p)
    }
  }
}

