package raw

import scala.collection.immutable.Seq

/** Types
  */
sealed abstract class Type extends RawNode

/** Primitive Types
  */
sealed abstract class PrimitiveType extends Type

case class BoolType() extends PrimitiveType

case class StringType() extends PrimitiveType

sealed abstract class NumberType extends PrimitiveType

case class IntType() extends NumberType

case class FloatType() extends NumberType

/** Product Type
  */
case class ProductType(tipes: Seq[Type]) extends Type

/** Record Type
  */
case class AttrType(idn: String, tipe: Type) extends RawNode

case class RecordType(atts: Seq[AttrType]) extends Type {
  def idns = atts.map(_.idn)

  // TODO: Reuse this impl elsewhere
    def typeOf(attribute: String): Type = {
      val matches = atts.filter {
        case c: AttrType => c.idn == attribute
      }
      matches.head.tipe
    }
}

//
//case class RecordType(tipes: Seq[Type], idns: Option[Seq[String]]) extends Type {
//  idns match {
//    case Some(idns) => if (tipes.length != idns.length) throw new IllegalArgumentException("invalid number of identifiers")
//  }

//  def typeOf(attribute: String): Type = {
//    val matches = atts.filter {
//      case c: AttrType => c.idn == attribute
//    }
//    matches.head.tipe
//  }

/** Collection Types
  */
abstract class CollectionType extends Type {
  def innerType: Type
}

case class BagType(innerType: Type) extends CollectionType
case class ListType(innerType: Type) extends CollectionType
case class SetType(innerType: Type) extends CollectionType

/** Class Type
  */
case class ClassType(idn: String) extends Type

/** Function Type `t2` -> `t1`
  */
case class FunType(t1: Type, t2: Type) extends Type

/** Type Variable
  */
case class TypeVariable(tipes: Set[Type] = Set()) extends Type

object Types {

  def intersect(t1: Type, t2: Type): Type = {
    def apply(t1: Type, t2: Type): Type = (t1, t2) match {
      case (tA: PrimitiveType, tB: PrimitiveType) if tA == tB => tA
      case (FunType(tA1, tA2), FunType(tB1, tB2))             => FunType(apply(tA1, tB1), apply(tA2, tB2))
      case (BagType(tA), BagType(tB))                         => BagType(apply(tA, tB))
      case (ListType(tA), ListType(tB))                       => ListType(apply(tA, tB))
      case (SetType(tA), SetType(tB))                         => SetType(apply(tA, tB))
      case (r1: RecordType, r2: RecordType) =>
        RecordType(
          r1.atts.filterNot{ case att => r2.idns.contains(att.idn) } ++
            r2.atts.filterNot{ case att => r1.idns.contains(att.idn) } ++
            (for (att1 <- r1.atts; att2 <- r2.atts; if att1.idn == att2.idn) yield AttrType(att1.idn, apply(att1.tipe, att2.tipe))))
      case (TypeVariable(tipes1), TypeVariable(tipes2)) =>
        val tipes = for (t1 <- tipes1; t2 <- tipes2) yield (t1, t2)
        TypeVariable(tipes.map{case (t1, t2) => apply(t1, t2)}.toSet.filter{_ != TypeVariable()})
      case (t1: TypeVariable, t2) => apply(t1, TypeVariable(Set(t2)))
      case (t1, t2: TypeVariable) => apply(TypeVariable(Set(t1)), t2)
      case _ => TypeVariable()
    }

    apply(t1, t2) match {
      case TypeVariable(tipes) if tipes.size == 1 => tipes.head
      case t                                      => t
    }
  }

  def compatible(t1: Type, t2: Type): Boolean = (t1, t2) match {
      // TODO: Find a more readable syntax! Create extractor object?
    case (TypeVariable(s), _) if s.isEmpty => true
    case (_, TypeVariable(s)) if s.isEmpty => true
    case (t1, t2)                          => intersect(t1, t2) != TypeVariable()
  }

}