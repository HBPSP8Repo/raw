package raw.mdcatalog

sealed abstract class AccessPath

case class SequentialAccessPath(location: Location, format: DataFormat, sizeInBytes: Option[Long]) extends AccessPath
