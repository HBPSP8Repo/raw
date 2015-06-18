package raw
package algebra

import org.kiama.util.PositionedParserUtilities
import raw.algebra.Expressions._
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode

/**
 * Created by gaidioz on 15.06.15.
 */
object LogicalAlgebraParser extends PositionedParserUtilities {

  def apply(input: String): Either[String, LogicalAlgebraNode] = parseAll(tree, input) match {
    case Success(ast, _) => Right(ast)
    case f => Left(f.toString)
  }

  lazy val tree: PackratParser[LogicalAlgebraNode] = {
    reduce | scan | select
  }

  lazy val scan: PackratParser[LogicalAlgebra.Scan] = {
    "Scan(" ~> idn ~ ("," ~> tipe) <~ ")" ^^ LogicalAlgebra.Scan
  }

  lazy val tipe: PackratParser[Type] = {
    "IntType()" ^^^ IntType() |
    "FloatType()" ^^^ FloatType() |
    "StringType()" ^^^ StringType() |
    "BoolType()" ^^^ BoolType() |
    "RecordType(Seq(" ~> attributes ~ (")," ~> idn) <~ ")" ^^ {
      case attrs ~ name => {
        if (name != "None") RecordType(attrs, Some(name)) else RecordType(attrs, None)
      }
    }
  }

  lazy val attributes: PackratParser[scala.collection.immutable.Seq[AttrType]] = repsep(attribute, ",")

  lazy val attribute: PackratParser[AttrType] = {
    "AttrType(" ~> idn ~ ("," ~> tipe) <~ ")" ^^ { case i ~ t => AttrType(i, t)}
  }

  lazy val select: PackratParser[LogicalAlgebra.Select] = {
    "Select(" ~> exp ~ ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Select
  }

  lazy val reduce: PackratParser[LogicalAlgebra.Reduce] = {
    "Reduce(" ~> monoid ~ ("," ~> exp) ~ ("," ~> exp) ~ ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Reduce
  }

  lazy val monoid: PackratParser[Monoid] = {
    "SetMonoid()" ^^^ SetMonoid() |
    "SumMonoid()" ^^^ SumMonoid() |
    "BagMonoid()" ^^^ BagMonoid() |
    prim_monoid
  }

  lazy val prim_monoid: PackratParser[PrimitiveMonoid] = {
    "AndMonoid()" ^^^ SumMonoid() |
    "MaxMonoid()" ^^^ MaxMonoid()
  }

  lazy val exp: PackratParser[Exp] = {
    "IntConst(" ~> number <~ ")" ^^ IntConst |
    "Arg(" ~> tipe <~ ")" ^^ Arg |
    "StringConst(" ~> string <~ ")" ^^ StringConst |
    "BoolConst(" ~> ("true"|"false") <~ ")" ^^ { case e => BoolConst(e.toBoolean) } |
    "BinaryExp(" ~> binop ~ ("," ~> exp) ~ ("," ~> exp) <~ ")" ^^ { case op ~ e1 ~ e2 => BinaryExp(op, e1, e2)} |
    "RecordProj(" ~> exp ~ ("," ~> idn) <~ ")" ^^ { case e ~ f => RecordProj(e, f) } |
    "RecordCons(" ~> attrconss <~ ")" ^^ RecordCons |
    "MergeMonoid(" ~> prim_monoid ~ ("," ~> exp) ~ ("," ~> exp) <~ ")" ^^ { case m ~ e1 ~ e2 => MergeMonoid(m, e1, e2)}
  }

  lazy val attrconss: PackratParser[scala.collection.immutable.Seq[AttrCons]] = repsep(attrcons, ",")

  lazy val attrcons: PackratParser[AttrCons] = {
    "AttrCons(" ~> idn ~ ("," ~> exp) <~ ")" ^^ AttrCons |
    "AttrCons(" ~> number ~ ("," ~> exp) <~ ")" ^^ { case n ~ t => AttrCons("_" + n.toString, t) }
  }

  lazy val binop: PackratParser[BinaryOperator] = {
    "Eq()" ^^^ Eq()
  }

  lazy val number: PackratParser[String] = "[0-9]+".r ^^ { case e => e.toString }
  lazy val idn: PackratParser[Idn] = "[a-zA-Z][a-zA-Z0-9]*".r ^^ { case e => e.toString }
  lazy val string =
    regex ("\"[^\"]*\"".r) ^^ {
      case s => s.substring (1, s.length - 1)
    }
}
