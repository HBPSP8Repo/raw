package raw.catalog

import raw.CollectionType

class Catalog(val catalog: Map[String, CollectionType]) {
  def hasClass(id: String) = catalog.isDefinedAt(id)
  
  def getClassType(id: String): CollectionType = catalog(id)
  
  def classNames: Set[String] = catalog.keys.toSet
}