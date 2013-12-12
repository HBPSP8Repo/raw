package raw.catalog

import raw.MonoidType

class Catalog(val catalog: Map[String, MonoidType]) {
  def hasClass(id: String) = catalog.isDefinedAt(id)
  
  def getClassType(id: String): MonoidType = catalog(id)
  
  def classNames: Set[String] = catalog.keys.toSet
}