package raw.mdcatalog

sealed abstract class DataFormat

case class CSV(properties: CsvDataSourceProperties) extends DataFormat

case class JSON() extends DataFormat

case class Text() extends DataFormat

case class Parquet() extends DataFormat // Placeholder for the future; no idea of properties yet