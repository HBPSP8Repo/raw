package raw.calculus

/** Calculus
  */
object Calculus {

  import org.kiama.util.TreeNode

  sealed abstract class CalculusNode extends TreeNode

  /** Identifiers are represented as strings
    */
  type Idn = String

  /** Qualifier
    */
  sealed abstract class Qual extends CalculusNode

  /** Expressions
    */
  sealed abstract class Exp extends Qual

  /** Null
    */
  case class Null() extends Exp {
    override def toString() = "null"
  }

  /** Constants
    */
  sealed abstract class Const extends Exp {
    type T

    def value: T

    override def toString() = s"$value"
  }

  // TODO: Add DateTime, smaller/larger integers/floats.
  case class BoolConst(value: Boolean) extends Const {
    type T = Boolean
  }

  case class IntConst(value: Integer) extends Const {
    type T = Integer
  }

  case class FloatConst(value: Float) extends Const {
    type T = Float
  }

  case class StringConst(value: String) extends Const {
    type T = String
  }

  /** Identifier reference
    */
  sealed abstract class IdnNode extends CalculusNode {
    def idn: String

    override def toString() = idn
  }

  /** Defining occurrence of an identifier
    */
  case class IdnDef(idn: Idn) extends IdnNode

  /** Use of an identifier
    */
  case class IdnUse(idn: Idn) extends IdnNode

  /** Identifier expression
    */
  case class IdnExp(name: IdnUse) extends Exp {
    override def toString() = name.toString()
  }

  /** Record Projection
    */
  case class RecordProj(e: Exp, idn: Idn) extends Exp {
    override def toString() = s"$e.$idn"
  }

  /** Record Construction
    */
  case class AttrCons(idn: Idn, e: Exp) extends CalculusNode {
    override def toString() = s"$idn := $e"
  }

  case class RecordCons(atts: Seq[AttrCons]) extends Exp {
    override def toString() = s"(${atts.mkString(", ")})"
  }

  /** If/Then/Else
    */
  case class IfThenElse(e1: Exp, e2: Exp, e3: Exp) extends Exp {
    override def toString() = s"if $e1 then $e2 else $e3"
  }

  /** Binary Expression
    */
  case class BinaryExp(op: BinaryOperator, e1: Exp, e2: Exp) extends Exp {
    override def toString() = s"$e1 $op $e2"
  }

  /** Function Application
    */
  case class FunApp(f: Exp, e: Exp) extends Exp {
    override def toString() = s"$f($e)"
  }

  /** Zero for Collection Monoid
    */
  case class ZeroCollectionMonoid(m: CollectionMonoid) extends Exp {
    override def toString() = s"${m.openSym}${m.closeSym}"
  }

  /** Construction for Collection Monoid
    */
  case class ConsCollectionMonoid(m: CollectionMonoid, e: Exp) extends Exp {
    override def toString() = s"${m.openSym} $e ${m.closeSym}"
  }

  /** Merge Monoid
    */
  case class MergeMonoid(m: Monoid, e1: Exp, e2: Exp) extends Exp {
    override def toString() = s"$e1 ${m.mergeSym} $e2"
  }

  /** Comprehension
    */
  case class Comp(m: Monoid, qs: Seq[Qual], e: Exp) extends Exp {
    override def toString() = s"for (${qs.map(_.toString()).mkString(", ")}) yield ${m.compSym} $e"
  }

  /** Unary Expression
    */
  case class UnaryExp(op: UnaryOperator, e: Exp) extends Exp {
    override def toString() = s"$op $e"
  }

  /** Function Abstraction
    */
  case class FunAbs(idn: IdnDef, t: Type, e: Exp) extends Exp {
    override def toString() = s"\\$idn: $t => $e"
  }

  /** Statements
    */
  sealed abstract class Statement extends Qual

  /** Generator
    */
  case class Gen(idn: IdnDef, e: Exp) extends Statement {
    override def toString() = s"$idn <- $e"
  }

  /** Bind
    */
  case class Bind(idn: IdnDef, e: Exp) extends Statement {
    override def toString() = s"$idn := $e"
  }

}