//package raw.calculus
//
//import org.kiama.util.Environments
//
//object SymbolTable extends Environments {
//
//  import org.kiama.util.{Counter, Entity}
//  import raw.Variable
////  import raw.Type
////  import Calculus.Exp
//
//  val counter = new Counter(0)
//
//  /** Return the next unique identifier.
//    */
//  def next(): String = {
//    val n = counter.value
//    counter.next()
//    s"$$$n"
//  }
//
//  /** Reset the symbol table.
//   */
//  def reset() {
//    counter.reset()
//  }
//
//  case class RawEntity(v: Variable) extends Entity {
//    /** Unique identifier for an entity.
//      */
//    val id = next()
//  }
//
////  sealed abstract class RawEntity extends Entity {
////    /** Unique identifier for an entity.
////      */
////    val id = next()
////  }
////
////  /** A bound variable entity.
////    */
////  case class BindEntity(e: Exp) extends RawEntity
////
////  /** A generator variable entity.
////    */
////  case class GenEntity(e: Exp) extends RawEntity
////
////  /** An entity with an associated variable.
////    */
////  sealed abstract class TypedEntity extends RawEntity {
////    var inferredType: Type
////  }
////
//////  /** A null symbol.
//////    */
//////  case class NullSymbol() extends TypedEntity
////
////  /** A class entity.
////   */
////  case class ClassEntity(name: String, tipe: Type) extends RawEntity
//
//}