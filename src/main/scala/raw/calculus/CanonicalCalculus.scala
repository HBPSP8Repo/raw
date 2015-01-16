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
  case class Null() extends Exp

  /** Constants
    */
  sealed abstract class Const extends Exp {
    type T
    def value: T
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
  }

  /** Record Projection
    */
  case class RecordProj(e: Exp, idn: Idn) extends Exp

  /** Record Construction
    */
  case class AttrCons(idn: Idn, e: Exp) extends CalculusNode
  case class RecordCons(atts: Seq[AttrCons]) extends Exp

  /** If/Then/Else
    */
  case class IfThenElse(e1: Exp, e2: Exp, e3: Exp) extends Exp

  /** Binary Expression
    */
  case class BinaryExp(op: BinaryOperator, e1: Exp, e2: Exp) extends Exp

  /** Zero for Collection Monoid
    */
  case class ZeroCollectionMonoid(m: CollectionMonoid) extends Exp

  /** Construction for Collection Monoid
    */
  case class ConsCollectionMonoid(m: CollectionMonoid, e: Exp) extends Exp

  /** Merge Monoid
    */
  case class MergeMonoid(m: Monoid, e1: Exp, e2: Exp) extends Exp

  /** Comprehension in canonical form, i.e. with paths and predicates in CNF.
    *
    * For details in the canonical form, refer to [1] page 19.
    */
  case class Comp(m: Monoid, paths: List[Gen], preds: List[Exp], e: Exp) extends Exp

  /** Unary Expression
    */
  case class UnaryExp(op: UnaryOperator, e: Exp) extends Exp

  /** Generator in canonical form, i.e. using paths.
    */
  case class Gen(v: Var, p: Path) extends CalculusNode

}
