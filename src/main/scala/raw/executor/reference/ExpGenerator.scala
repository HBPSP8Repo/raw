package raw
package executor
package reference

import com.typesafe.scalalogging.LazyLogging
import raw.algebra.Expressions._
import raw.algebra.LogicalAlgebra.{Scan, LogicalAlgebraNode}
import raw.algebra.{ExpressionTyper, LogicalAlgebraTyper}

/**
 * Created by gaidioz on 3/6/15.
 */

object ExpGenerator extends LazyLogging {

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  protected val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def genPF(e: Exp, n: LogicalAlgebraNode): Any => Any = {

    def realType(t: raw.Type): String = t match {
      case IntType() => "Int"
      case FloatType() => "Float"
      case StringType() => "String"
      case BoolType() => "Boolean"
      case RecordType(atts) => "Map[String, Any]"
      case SetType(innerType) => s"Set[${realType(innerType)}"
      case ListType(innerType) => s"Set[${realType(innerType)}"
    }

    def recurse(e: Exp): String = e match {
      case IntConst(s) => s"($s)"
      case FloatConst(s) => s"($s)"
      case BoolConst(s) => s"($s)"
      case StringConst(s) => s""""${s}""""
      case Arg => "(arg)"
      case RecordProj(r, idn) => { val child = recurse(r) ; s"""($child("$idn").asInstanceOf[${realType(ExpressionTyper(e, n))}])""" }
      case RecordCons(attrs) => attrs.map(x => s""""${x.idn}" -> (${recurse(x.e)})""").mkString("(Map(", ",", "))")
      case IfThenElse(cond, thenExp, elseExp) => s"(if (${recurse(cond)}) { ${recurse(thenExp)} } else { ${recurse(elseExp)} })"
      case BinaryExp(op, e1, e2) => {
        val opSym = op match {
          case _ : Gt  => ">"
          case _ : Ge  => ">="
          case _ : Le  => "<="
          case _ : Lt  => "<"
          case _ : Eq  => "=="
          case _ : Neq => "!="
          case _ : Sub => "-"
          case _ : Div => "/"
          case _ : Mod => "%"
        }
        s"(${recurse(e1)} $opSym ${recurse(e2)})"
      }
      case UnaryExp(op, e) => {
        val opSym = op match {
          case _ : Not => "!%s"
          case _ : Neg => "!%s"
          case _ : ToBool => "%s.toBool"
          case _ : ToInt => "%s.toInt"
          case _ : ToFloat => "%s.toFloat"
          case _ : ToString => "%s.toString"
        }
        "(" + opSym.format(recurse(e)) + ")"
      }
      case MergeMonoid(m, e1, e2) => {
        val opSym = m match {
          case _: MaxMonoid => "max(%s, %s)"
          case _: SumMonoid => "%s + %s"
          case _: SetMonoid => "%s + %s"
          case _: ListMonoid => "%s ++ %s"
        }
        "(" + opSym.format(recurse(e1), recurse(e2)) + ")"
      }
    }

    val argType = realType(LogicalAlgebraTyper(n))
    val returnType = realType(ExpressionTyper(e, n))
    val funcCode = s"(arg: ${argType}) => ${recurse(e)}"
    logger.debug(funcCode)
    toolbox.compile(toolbox.parse(funcCode))().asInstanceOf[Any => Any]
  }

  def genPred(e: Exp, n: LogicalAlgebraNode) = genPF(e, n).asInstanceOf[Any => Boolean]
}

object ExpMain {

  def main(args: Array[String]) {
    val e = RecordCons(List(AttrCons("r", MergeMonoid(SumMonoid(), IntConst("2"), RecordProj(Arg, "a")))))
    val data = List(Map("a" -> 2, "b" -> "tralala"))
    val s = ExpGenerator.genPF(e, Scan(data, RecordType(List(AttrType("a", IntType()), AttrType("b", StringType())))))
    println(data.map(s))
  }
}