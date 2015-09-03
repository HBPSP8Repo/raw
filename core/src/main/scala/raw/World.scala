package raw

import scala.collection.immutable.Map
import raw.calculus.{TypesPrettyPrinter, Symbol}

import scala.util.parsing.input.Position
import com.typesafe.scalalogging.LazyLogging

class Group(val root: Type, val tipes: Set[Type])


/** World
  */
class World(val sources: Map[String, Type] = Map(),
            val tipes: Map[Symbol, Type] = Map())

object World extends LazyLogging {

  import calculus.Calculus.Exp

  //type VarMap = Map[Symbol, Type]

  //type VarMap = Map[Meta, Group]

  class VarMap(val m: List[(Type, Group)] = List(), val query: Option[String] = None) {

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

        // Pretty-print changes to the map
        query match {
          case Some(q) => logger.debug("Map change\n" + printMap(q, ntipes.map(_.pos), ng.root))
          case None =>
        }

        new VarMap(nm.toList, query=query)
      }
    }

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

    def printMap(q: String, pos: Set[Position], t: Type) = {
      val posPerLine = pos.groupBy(_.line)
      var output = s"New Type: ${TypesPrettyPrinter(t)}\n"
      for ((line, lineno) <- q.split("\n").zipWithIndex) {
        output += line + "\n"
        if (posPerLine.contains(lineno + 1)) {
          val cols = posPerLine(lineno + 1).map(_.column).toList.sortWith(_ < _)
          var c = 0
          for (col <- cols) {
            output += " " * (col - c - 1)
            output += "^"
            c = col
          }
          output += "\n"
        }
      }
      output
    }

  }

}