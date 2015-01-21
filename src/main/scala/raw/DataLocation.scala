package raw

/**
 * Created by gaidioz on 1/21/15.
 */

abstract class DataLocation
// case class LocalFileLocation("/path/to/csv"), etc.
case class MemoryLocation(source: List[Any]) extends DataLocation
