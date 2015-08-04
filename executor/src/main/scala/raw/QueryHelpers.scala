package raw

/**
 * Helper functions used by the generated code.
 *
 * When refactoring this class, be sure to manually update the corresponding usages in the
 * quasiquotes used
 */
object QueryHelpers {

  import scala.collection.immutable.Bag
  import scala.reflect.ClassTag

  def bagBuilder[T: ClassTag](map: scala.collection.Map[T, Long]): Bag[T] = {
    implicit val bagConfig = Bag.configuration.compact[T]
    val builder = Bag.newBuilder
    map.foreach(v => builder.add(v._1, v._2.toInt))
    builder.result()
  }
}
