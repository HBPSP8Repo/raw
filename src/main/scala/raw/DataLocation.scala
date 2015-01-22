package raw

sealed abstract class DataLocation

// TODO: Create hierarchy for `fileType` or use some "default" convention, e.g. mime-type.
case class LocalFileLocation(path: String, fileType: String) extends DataLocation

// TODO: Convert from List to Seq and add second parameter with Type (SetType, ...) so that we cast appropriately?
case class MemoryLocation(data: Iterable[Any]) extends DataLocation

case object EmptyLocation extends DataLocation
