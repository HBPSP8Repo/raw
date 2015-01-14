package raw.calculus

/** CanonicalCalculus
  */
object CanonicalCalculus {

  import org.kiama.util.{ Entity, TreeNode }
  import org.kiama.util.Counter

  val varCounter = new Counter(0)

  sealed abstract class CalculusNode extends TreeNode

  /** Identifiers are represented as strings
    */
  type Idn = String

  /** Expressions
    */
  sealed abstract class Exp extends CalculusNode

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

  /** Variable
    */
  case class Var() extends Exp {
    val locn = {
      val loc = varCounter.value
      varCounter.next()
      loc
    }

    /** Overriding `equals` and `hashCode` so that variables are distinguished on `==`.
      */
    override def equals(o: Any) = super.equals(o)
    override def hashCode = super.hashCode

    override def toString() = s"var$locn"
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

  /** Comprehension in canonical form, i.e. with paths and predicates in CNF.
    *
    * For details in the canonical form, refer to [1] page 19.
    */
  case class Comp(m: Monoid, paths: List[Gen], preds: List[Exp], e: Exp) extends Exp {
    override def toString() = s"for (${(paths ++ preds).map(_.toString()).mkString(", ")}) yield ${m.compSym} $e"
  }

  /** Unary Expression
    */
  case class UnaryExp(op: UnaryOperator, e: Exp) extends Exp {
    override def toString() = s"$op $e"
  }

  /** Generator in canonical form, i.e. using paths.
    */
  case class Gen(v: Var, p: Path) extends CalculusNode {
    override def toString() = s"$v <- $p"
  }

}