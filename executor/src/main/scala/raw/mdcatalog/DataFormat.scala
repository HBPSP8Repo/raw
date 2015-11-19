package raw.mdcatalog

import java.util

case class CsvDataSourceProperties(hasHeader: Option[Boolean], fieldNames: Option[util.ArrayList[String]], delimiter: Option[Char])


sealed abstract class DataFormat

case class CSV(properties: CsvDataSourceProperties) extends DataFormat

case class JSON() extends DataFormat

case class Text() extends DataFormat

case class Parquet() extends DataFormat
