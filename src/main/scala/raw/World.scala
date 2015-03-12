package raw

case class LocationError(err: String) extends RawException(err)

class World(val catalog: Map[String, DataLocation] = Map(), val userTypes: Map[String, Type] = Map()) {
  def getLocation(name: String): DataLocation = catalog.get(name) match {
    case Some(d: DataLocation) => d
    case None => throw LocationError(s"Unknown location $name")
  }
}
