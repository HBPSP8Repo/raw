package raw

import scala.collection.immutable.Map
import raw.calculus.{FriendlierPrettyPrinter, Symbol}

import com.typesafe.scalalogging.LazyLogging

class Group[A](val root: A, val elements: Set[A])


/** World
  */
class World(val sources: Map[String, Type] = Map(),
            val tipes: Map[Symbol, Type] = Map())

object World extends LazyLogging {

  abstract class VarMap[A] {

    protected var m: List[(A, Group[A])] = List()

    def groupEq(e1: A, e2: A): Boolean

    def get(t: A): Option[Group[A]] = {
      for ((k, g) <- m) {
        if (groupEq(k, t))
          return Some(g)
      }
      None
    }

    def contains(t: A): Boolean = get(t).isDefined

    def getOrElse(t: A, g: Group[A]) = get(t).getOrElse(g)

    def apply(t: A): Group[A] = get(t).head

    def union(t1: A, t2: A): VarMap[A] = {
      logger.debug(s"t1 is $t1 and t2 is $t2")


      val g1 = getOrElse(t1, new Group[A](t1, Set(t1)))
      if (!contains(t2)) {
        val ng = new Group(t2, g1.elements + t2)
        val nm = scala.collection.mutable.MutableList[(A, Group[A])]()
        for ((k, g) <- m) {
          var found = false
          for (k1 <- g1.elements) {
            if (groupEq(k, k1)) {
              found = true
            }
          }
          if (!found) {
            nm += ((k, g))
          }
        }
        for (k <- ng.elements) {
          nm += ((k, ng))
        }
        m = nm.toList
      } else {
        val g2 = apply(t2)
        if (!(g1 eq g2)) {
          val ntipes = g1.elements union g2.elements
          val ng = new Group(t2, ntipes)
          assert(ng.elements.contains(t1))
          assert(ng.elements.contains(t2))
          val nm = scala.collection.mutable.MutableList[(A, Group[A])]()
          for ((k, g) <- m) {
            var found = false
            for (k1 <- ntipes) {
              if (groupEq(k, k1)) {
                found = true
              }
            }
            if (!found) {
              nm += ((k, g))
            }
          }
          for (k <- ntipes) {
            nm += ((k, ng))
          }

          m = nm.toList
        }
      }
      this
    }

    def getRoots: Set[A] =
      m.map(_._2).map(_.root).toSet

    def keys: List[A] =
      m.map(_._1)

  }

  class TypesVarMap extends VarMap[Type] {

    def typeVariables: Set[TypeVariable] =
      m.collect { case (v: TypeVariable, _) => v }.toSet

    /** Equality between groups.
      * UserTypes are handled as value equality instead of reference equality because in the test code, we create
      * the UserType multiple times (e.g we use UserType(Symbol("Department")) multiple times, instead of creating it
      * one and using the same object.
      */
    def groupEq(t1: Type, t2: Type): Boolean = (t1, t2) match {
      case (_: UserType, _: UserType) => t1 == t2
      case (_: TypeVariable, _: TypeVariable) => t1 == t2
      case (_: NumberType, _: NumberType) => t1 == t2
      case _ => t1 eq t2
    }

    def getSymbols: Set[Symbol] =
      m.flatMap { case (v: VariableType, _) => List(v.sym) case _ => Nil }.toSet

    override def toString: String = {
      var s = "\n"
      val keys = m.map(_._1).sortBy(_.toString)
      for (k <- keys) {
        val g = apply(k)
        s += s"${FriendlierPrettyPrinter(k)} => ${FriendlierPrettyPrinter(g.root)} (${
          g.elements.map {
            case t: VariableType => s"[${t.sym.idn}] ${FriendlierPrettyPrinter(t)}"
            case t => FriendlierPrettyPrinter(t)
          }.mkString(", ")
        })\n"
      }
      s
    }
  }

  class MonoidsVarMap extends VarMap[Monoid] {

    def groupEq(e1: Monoid, e2: Monoid) = (e1, e2) match {
      case (_: PrimitiveMonoid, _: PrimitiveMonoid) => e1 == e2
      case (_: SetMonoid, _: SetMonoid) => e1 == e2
      case (_: BagMonoid, _: BagMonoid) => e1 == e2
      case (_: ListMonoid, _: ListMonoid) => e1 == e2
      case (_: MonoidVariable, _: MonoidVariable) => e1 == e2
      case _ => e1 eq e2
    }

    override def toString: String = {
      var s = "\n"
      val keys = m.map(_._1).sortBy(_.toString)
      for (k <- keys) {
        val g = apply(k)
        s += s"${PrettyPrinter(k)} => ${PrettyPrinter(g.root)} (${
          g.elements.map(PrettyPrinter(_)).mkString(", ")
        })\n"
      }
      s
    }
  }

  class RecordAttributesVarMap extends VarMap[RecordAttributes] {

    def groupEq(g1: RecordAttributes, g2: RecordAttributes): Boolean = (g1, g2) match {
      case (_: AttributesVariable, _: AttributesVariable) => g1 == g2
      case (_: ConcatAttributes, _: ConcatAttributes) => g1 == g2
      case _ => g1 eq g2
    }

    override def toString: String = {
      var s = "\n"
      val keys = m.map(_._1).sortBy(_.toString)
      for (k <- keys) {
        val g = apply(k)
        s += s"${PrettyPrinter(k)} => ${PrettyPrinter(g.root)} (${
          g.elements.map(PrettyPrinter(_)).mkString(", ")
        })\n"
      }
      s
    }
  }

}