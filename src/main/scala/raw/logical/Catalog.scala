package raw.logical

import raw._

class Catalog(val catalog: Map[String, MonoidType]) {
  def getType(name: String) = catalog(name)  
}