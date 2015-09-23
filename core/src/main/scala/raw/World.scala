package raw

import scala.collection.immutable.Map
import raw.calculus.{CalculusPrettyPrinter, TypesPrettyPrinter, Symbol}

import scala.util.parsing.input.Position
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

    def union(t1: A, t2: A) = {
      val g1 = getOrElse(t1, new Group[A](t1, Set(t1)))
      val g2 = getOrElse(t2, new Group[A](t2, Set(t2)))
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
      this
    }

    def getRoots: Set[A] =
      m.map(_._2).map(_.root).toSet

  }

  class TypesVarMap extends VarMap[Type] {

    def typeVariables: Set[TypeVariable] =
      m.collect { case (v: TypeVariable, _) => v }.toSet

    def groupEq(t1: Type, t2: Type): Boolean = (t1, t2) match {
      case (_: UserType, _: UserType) => t1 == t2
      case (_: TypeVariable, _: TypeVariable) => t1 == t2
      case _ => t1 eq t2
    }

    def getSymbols: Set[Symbol] =
      m.flatMap { case (v: VariableType, _) => List(v.sym) case _ => Nil }.toSet

    override def toString: String = {
      var s = "\n"
      val keys = m.map(_._1).sortBy(_.toString)
      for (k <- keys) {
        val g = apply(k)
        s += s"${TypesPrettyPrinter(k)} => ${TypesPrettyPrinter(g.root)} (${
          g.elements.map {
            case t: VariableType => s"[${t.sym.idn}] ${TypesPrettyPrinter(t)}"
            case t => TypesPrettyPrinter(t)
          }.mkString(", ")
        })\n"
      }
      s
    }
  }

  class MonoidsVarMap extends VarMap[CollectionMonoid] {
    def groupEq(e1: CollectionMonoid, e2: CollectionMonoid) = (e1, e2) match {
      case (_: SetMonoid, _: SetMonoid) => e1 == e2
      case (_: BagMonoid, _: BagMonoid) => e1 == e2
      case (_: ListMonoid, _: ListMonoid) => e1 == e2
      case _ => e1 eq e2
    }
    override def toString: String = {
      var s = "\n"
      val keys = m.map(_._1).sortBy(_.toString)
      for (k <- keys) {
        val g = apply(k)
        s += s"${PrettyPrinter(k)} => ${PrettyPrinter(g.root)} (${
          g.elements.map {
            case t: MonoidVariable => s"[${t.sym.idn}] ${PrettyPrinter(t)}"
            case t => PrettyPrinter(t)
          }.mkString(", ")
        })\n"
      }
      s
    }
  }

}