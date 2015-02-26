package raw

sealed abstract class DataLocation

case class LocalFileLocation(path: String, fileType: String) extends DataLocation

case class MemoryLocation(data: Iterable[Any]) extends DataLocation

case object EmptyLocation extends DataLocation
