package raw

sealed abstract class DataLocation {
  def tipe: CollectionType
}

sealed abstract class MimeType(tipe:String)
case object TEXT_CSV extends MimeType("text/csv")
case object APPLICATION_JSON extends MimeType("application/json")

case class LocalFileLocation(tipe: CollectionType, path: String, fileType: MimeType) extends DataLocation

case class MemoryLocation(tipe: CollectionType, data: Iterable[Any]) extends DataLocation

// TODO: Consider dropping EmptyLocation
case object EmptyLocation extends DataLocation {
  def tipe = ???
}
