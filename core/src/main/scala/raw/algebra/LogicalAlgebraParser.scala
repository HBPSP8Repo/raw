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
    reduce | assign | scan | select | nest | unnest | join
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
    } |
    "BagType(" ~> tipe <~ ")" ^^ BagType |
    "SetType(" ~> tipe <~ ")" ^^ SetType |
    "ListType(" ~> tipe <~ ")" ^^ ListType
  }

  lazy val assignments: PackratParser[scala.collection.immutable.Seq[Tuple2[String, LogicalAlgebraNode]]] = repsep(assignment, ",")
  lazy val assignment: PackratParser[Tuple2[String, LogicalAlgebraNode] ] = {
    "Bind(" ~> idn ~ ("," ~> tree) <~ ")" ^^ { case i ~ t => Tuple2(i, t) }
  }

  lazy val attributes: PackratParser[scala.collection.immutable.Seq[AttrType]] = repsep(attribute, ",")

  lazy val attribute: PackratParser[AttrType] = {
    "AttrType(" ~> idn ~ ("," ~> tipe) <~ ")" ^^ { case i ~ t => AttrType(i, t)} |
    "AttrType(" ~> number ~ ("," ~> tipe) <~ ")" ^^ { case n ~ t => AttrType("_" + n.toString, t)}
  }

  lazy val select: PackratParser[LogicalAlgebra.Select] = {
    "Select(" ~> exp ~ ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Select
  }

  lazy val assign: PackratParser[LogicalAlgebra.Assign] = {
    "Assign(Seq(" ~> assignments ~ (")" ~>) ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Assign
  }

  lazy val reduce: PackratParser[LogicalAlgebra.Reduce] = {
    "Reduce(" ~> monoid ~ ("," ~> exp) ~ ("," ~> exp) ~ ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Reduce
  }

  lazy val join: PackratParser[LogicalAlgebra.Join] = {
    "Join(" ~> exp ~ ("," ~> tree) ~ ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Join
  }

  lazy val unnest: PackratParser[LogicalAlgebra.Unnest] = {
    "Unnest(" ~> exp ~ ("," ~> exp) ~ ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Unnest
  }

  lazy val nest: PackratParser[LogicalAlgebra.Nest] = {
    "Nest(" ~> monoid ~ ("," ~> exp) ~ ("," ~> exp) ~ ("," ~> exp) ~ ("," ~> exp) ~ ("," ~> tree) <~ ")" ^^ LogicalAlgebra.Nest
  }

  lazy val coll_monoid: PackratParser[CollectionMonoid] = {
    "SetMonoid()" ^^^ SetMonoid() |
    "BagMonoid()" ^^^ BagMonoid() |
    "ListMonoid()" ^^^ ListMonoid()
  }

  lazy val prim_monoid: PackratParser[PrimitiveMonoid] = {
    "OrMonoid()" ^^^ OrMonoid() |
    "AndMonoid()" ^^^ AndMonoid() |
    "SumMonoid()" ^^^ SumMonoid() |
    "MaxMonoid()" ^^^ MaxMonoid()
  }

  lazy val monoid: PackratParser[Monoid] = {
    coll_monoid |
    prim_monoid
  }

  lazy val exp: PackratParser[Exp] = {
    "IntConst(" ~> number <~ ")" ^^ IntConst |
    "Arg(" ~> tipe <~ ")" ^^ Arg |
    "StringConst(" ~> string <~ ")" ^^ StringConst |
    "BoolConst(" ~> ("true"|"false") <~ ")" ^^ { case e => BoolConst(e.toBoolean) } |
    "BinaryExp(" ~> binop ~ ("," ~> exp) ~ ("," ~> exp) <~ ")" ^^ { case op ~ e1 ~ e2 => BinaryExp(op, e1, e2)} |
    "UnaryExp(" ~> unaryop ~ ("," ~> exp) <~ ")" ^^ { case op ~ e => UnaryExp(op, e)} |
    "RecordProj(" ~> exp ~ ("," ~> idn) <~ ")" ^^ { case e ~ f => RecordProj(e, f) } |
    "RecordProj(" ~> exp ~ ("," ~> number) <~ ")" ^^ { case e ~ f => RecordProj(e, "_" + f.toString) } |
    "RecordCons(Seq(" ~> attrconss <~ "))" ^^ RecordCons |
    "ZeroCollectionMonoid(" ~> coll_monoid <~ ")" ^^ { case m => ZeroCollectionMonoid(m)} |
    "ConsCollectionMonoid(" ~> coll_monoid ~ ("," ~> exp) <~ ")" ^^ { case m ~ e => ConsCollectionMonoid(m, e)} |
    "MergeMonoid(" ~> monoid ~ ("," ~> exp) ~ ("," ~> exp) <~ ")" ^^ { case m ~ e1 ~ e2 => MergeMonoid(m, e1, e2)}
  }

  lazy val attrconss: PackratParser[scala.collection.immutable.Seq[AttrCons]] = repsep(attrcons, ",")

  lazy val attrcons: PackratParser[AttrCons] = {
    "AttrCons(" ~> idn ~ ("," ~> exp) <~ ")" ^^ AttrCons |
    "AttrCons(" ~> number ~ ("," ~> exp) <~ ")" ^^ { case n ~ t => AttrCons("_" + n.toString, t) }
  }

  lazy val binop: PackratParser[BinaryOperator] = {
    "Lt()" ^^^ Lt() |
    "Le()" ^^^ Le() |
    "Gt()" ^^^ Gt() |
    "Ge()" ^^^ Ge() |
    "Eq()" ^^^ Eq() |
    "Neq()" ^^^ Neq() |
    "Sub()" ^^^ Sub() |
    "Div()" ^^^ Div() |
    "Mod()" ^^^ Mod()
  }

  lazy val unaryop: PackratParser[UnaryOperator] = {
    "Not()" ^^^ Not() |
    "Neg()" ^^^ Neg() |
    "ToInt()" ^^^ ToInt() |
    "ToFloat()" ^^^ ToFloat() |
    "ToBool()" ^^^ ToBool() |
    "ToString()" ^^^ ToString()
  }

  lazy val number: PackratParser[String] = "[0-9]+".r ^^ { case e => e.toString }

  lazy val idn: PackratParser[Idn] = "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { case e => e.toString }

  lazy val string =
    regex ("\"[^\"]*\"".r) ^^ {
      case s => s.substring (1, s.length - 1)
    }
}
