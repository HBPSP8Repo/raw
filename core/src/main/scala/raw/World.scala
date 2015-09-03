package raw

import scala.collection.immutable.Map
import raw.calculus.{TypesPrettyPrinter, Symbol}

class Group(val root: Type, val tipes: Set[Type])


/** World
  */
class World(val sources: Map[String, Type] = Map(),
            val tipes: Map[Symbol, Type] = Map())

object World {

  //type VarMap = Map[Symbol, Type]

  //type VarMap = Map[Meta, Group]

  class VarMap(val m: List[(Type, Group)] = List()) {

    def typeVariables: Set[TypeVariable] =
      m.collect { case (v: TypeVariable, _) => v }.toSet

    def typesEq(t1: Type, t2: Type): Boolean = (t1, t2) match {
      case (_: UserType, _: UserType) => t1 == t2
      case (_: TypeVariable, _: TypeVariable) => t1 == t2
      case _ => t1 eq t2
    }

    def get(t: Type): Option[Group] = {
      for ((k, g) <- m) {
        if (typesEq(k, t))
          return Some(g)
      }
      None
    }

    def contains(t: Type): Boolean = get(t).isDefined

    def getOrElse(t: Type, g: Group) = get(t).getOrElse(g)

    def apply(t: Type): Group = get(t).head

//    def ++(other: List[(Type, Group)]): VarMap = {
//      val nm = scala.collection.mutable.MutableList[(Type, Group)]()
//      for ((k, g) <- other) {
//        nm += ((k, g))
//      }
//      for ((k, g) <- this.m) {
//        if (!nm.contains(k)) {
//          nm += ((k, g))
//        }
//      }
//      new VarMap(nm.toList)
//    }
//
//    def ++(other: VarMap): VarMap = ++(other.m)

    def union(t1: Type, t2: Type): VarMap = {
      val g1 = getOrElse(t1, new Group(t1, Set(t1)))
      val g2 = getOrElse(t2, new Group(t2, Set(t2)))
      if (g1 eq g2) {
        assert(g1.tipes.contains(t1))
        assert(g2.tipes.contains(t2))
        this
      } else {
        val ntipes = g1.tipes union g2.tipes
        val ng = new Group(t2, ntipes)
        assert(ng.tipes.contains(t1))
        assert(ng.tipes.contains(t2))
        //        logger.debug(s"m is $m")
        //        logger.debug(s"n is ${ntipes.collect { case v: VariableType => v.sym }.map(_ -> ng).toMap}")
//        m ++ ntipes.collect { case v: VariableType => v }.map(_ -> ng).toList

        val nm = scala.collection.mutable.MutableList[(Type, Group)]()
        for ((k, g) <- m) {
          var found = false
          for (k1 <- ntipes) {
            if (typesEq(k, k1)) {
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
        new VarMap(nm.toList)
      }
    }
//      t2 match {
//        case t2: VariableType =>
//          else {
//          }
//        case _ =>
//          val ng = new Group(t2, g1.tipes + t2)
//          assert(ng.tipes.contains(t1))
//          assert(ng.tipes.contains(t2))
//          m ++ g1.tipes.collect { case v: VariableType => v }.map(_ -> ng).toList
//      }
//    }

    override def toString: String = {
      var s = "\n"
      val keys = m.map(_._1).sortBy(_.toString)
      for (k <- keys) {
        val g = apply(k)
        s += s"${TypesPrettyPrinter(k)} => ${TypesPrettyPrinter(g.root)} (${
          g.tipes.map {
            case t: VariableType => s"[${t.sym.idn}] ${TypesPrettyPrinter(t)}"
            case t => TypesPrettyPrinter(t)
          }.mkString(", ")
        })\n"
      }
      s
    }
  }

//  class VarMap {
//
//    make a list and go through it and do eq to every object ????
//    slowest thing ever
//
//    or make a map that points to a list of objects that have different eqs?
//
//    def contains(t: Type): Boolean = ???
//
//    def getOrElse(t: Type, g: Group): Group = ???
//
//    def apply(t: Type): Group = ???
//
//    def ++(other: VarMap) =
//      new VarMap()
//
//  }

//  class VarMap extends scala.collection.immutable.HashMap[Type, Group] {
//    protected override def elemHashCode(k: Type) = System.identityHashCode(k)
//
//    override def ++(other: VarMap): VarMap = super.++(other)
//  }
}