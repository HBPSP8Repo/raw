package raw

import scala.reflect.runtime.{universe => ru}
import scala.reflect.ClassTag
import raw.reflect.Reflector

/** Data Source.
  */
sealed abstract class DataSource {
  def tipe: Type
}

/** Empty data source used (for testcases only!).
  */
case class EmptyDataSource(tipe: Type) extends DataSource

/** Scala data source.
  *
  * Uses reflection to obtain Scala's type signature.
  */
case class ScalaDataSource[T](entity: T)(implicit tag: ru.TypeTag[T]) extends DataSource {
  // TODO: If type includes AnyType, this must be a type variable whose type must be inferred/unified!!!
  val tipe = Reflector.getRawType(entity)
}

/** World.
  */
class World(val userTypes: Map[String, Type] = Map(),
            val dataSources: Map[String, DataSource] = Map()) {

  /** Returns the type definition of `name`, if it exists.
    */
  def userType(name: String): Option[Type] = userTypes.get(name) match {
    case Some(t) => Some(t)
    case _       => None
  }

  /** Returns the data source with the given `name`, if it exists.
    */
  def dataSource(name: String): Option[DataSource] = dataSources.get(name) match {
    case Some(s) => Some(s)
    case _       => None
  }

}
