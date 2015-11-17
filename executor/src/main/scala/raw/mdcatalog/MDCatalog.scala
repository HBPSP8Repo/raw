package raw.mdcatalog

import scala.collection.mutable

object MDCatalog {
  private[this] val catalog = new mutable.HashMap[String, Source]()


  def register(name: String, source: Source): Unit = {
    catalog.put(name, source)
  }

  def lookup(name: String): Option[Source] = {
    catalog.get(name)
  }
}
