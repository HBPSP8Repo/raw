package raw.catalog

import raw.calculus.CollectionType

class Catalog(val catalog: Map[String, CollectionType]) {
  def hasClass(id: String) = catalog.isDefinedAt(id)
  def getClassType(id: String): CollectionType = catalog(id)
}