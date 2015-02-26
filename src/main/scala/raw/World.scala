package raw

case class LocationError(err: String) extends RawException(err)

case class Source(tipe: CollectionType, location: DataLocation)

class World(val catalog: Map[String, Source], val userTypes: Map[String, Type] = Map()) {
  def getSource(name: String): Source = catalog.get(name) match {
    case Some(Source(tipe, location)) => Source(tipe, location)
    case None => throw LocationError(s"Unknown location $name")
  }
}
