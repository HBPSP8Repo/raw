package raw

sealed abstract class DataLocation {
  def tipe: CollectionType
}

case class LocalFileLocation(tipe: CollectionType, path: String, fileType: MimeType) extends DataLocation

case class MemoryLocation(tipe: CollectionType, data: Iterable[Any]) extends DataLocation

// TODO: Consider dropping EmptyLocation
case object EmptyLocation extends DataLocation {
  def tipe = ???
}
