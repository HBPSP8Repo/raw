package raw
package algebra

case class AlgebraDSLError(err: String) extends RawException(err)

abstract class AlgebraLang {

  import scala.language.implicitConversions
  import scala.language.dynamics
  import scala.collection.immutable.Seq
  import LogicalAlgebra._
  import Expressions._

  /** World
    */
  val world: World

  /** Expression builders
    */
  sealed abstract class Builder extends Dynamic {

    /** Record Projection
      */
    def selectDynamic(idn: String) = RecordProjBuilder(this, idn)

    /** Binary Expressions
      */
    def ==(rhs: Builder) = BinaryExpBuilder(Eq(), this, rhs)

    def <>(rhs: Builder) = BinaryExpBuilder(Neq(), this, rhs)

    def >=(rhs: Builder) = BinaryExpBuilder(Ge(), this, rhs)

    def >(rhs: Builder) = BinaryExpBuilder(Gt(), this, rhs)

    def <=(rhs: Builder) = BinaryExpBuilder(Le(), this, rhs)

    def <(rhs: Builder) = BinaryExpBuilder(Lt(), this, rhs)

    def -(rhs: Builder) = BinaryExpBuilder(Sub(), this, rhs)

    def /(rhs: Builder) = BinaryExpBuilder(Div(), this, rhs)

    /** Merge Monoids
      */
    def max(rhs: Builder) = MergeMonoidBuilder(MaxMonoid(), this, rhs)

    def +(rhs: Builder) = MergeMonoidBuilder(SumMonoid(), this, rhs)

    def *(rhs: Builder) = MergeMonoidBuilder(MultiplyMonoid(), this, rhs)

    def unary_+(rhs: Builder) = MergeMonoidBuilder(SumMonoid(), this, rhs)

    def &&(rhs: Builder) = MergeMonoidBuilder(AndMonoid(), this, rhs)

    def ||(rhs: Builder) = MergeMonoidBuilder(OrMonoid(), this, rhs)

    def bag(rhs: Builder) = MergeMonoidBuilder(BagMonoid(), this, rhs)

    def list(rhs: Builder) = MergeMonoidBuilder(ListMonoid(), this, rhs)

    def set(rhs: Builder) = MergeMonoidBuilder(SetMonoid(), this, rhs)
  }

  case object NullBuilder extends Builder

  case class ConstBuilder(c: Const) extends Builder

  case object ArgBuilder extends Builder

  case class RecordProjBuilder(lhs: Builder, idn: String) extends Builder

  case class AttrConsBuilder(idn: String, b: Builder)

  case class RecordConsBuilder(atts: Seq[AttrConsBuilder]) extends Builder

  case class IfThenElseBuilder(i: IfThenElse) extends Builder

  case class BinaryExpBuilder(op: BinaryOperator, lhs: Builder, rhs: Builder) extends Builder

  case class MergeMonoidBuilder(m: Monoid, lhs: Builder, rhs: Builder) extends Builder

  case class UnaryExpBuilder(op: UnaryOperator, e: Builder) extends Builder

  /** Null
    */
  def nul() = NullBuilder

  /** Constants
    */
  implicit def boolToExp(v: Boolean): ConstBuilder = ConstBuilder(BoolConst(v))

  implicit def intToExp(v: Int): ConstBuilder = ConstBuilder(IntConst(v.toString))

  implicit def floatToExp(v: Float): ConstBuilder = ConstBuilder(FloatConst(v.toString))

  implicit def stringToExp(v: String): ConstBuilder = ConstBuilder(StringConst(v))

  /** Variable
    */
  //def arg(i: Int = -1) = if (i < 0) ArgBuilder else RecordProjBuilder(ArgBuilder, s"_${i + 1}")
  def arg = ArgBuilder

  /** Record Construction
    */
  def record(atts: Tuple2[String, Builder]*) = {
    val atts1 = Seq(atts:_*)
    RecordConsBuilder(atts1.map { case att => AttrConsBuilder(att._1, att._2)})
  }

  /** If `e1` Then `e2` Else `e3`
    */
  case class ThenElse(e2: Exp, e3: Exp)

  def ?(e1: Exp, thenElse: ThenElse) = IfThenElse(e1, thenElse.e2, thenElse.e3)

  def unary_:(e2: Exp, e3: Exp) = ThenElse(e2, e3)

  /** Zero Collection Monoids
    */

  /** Construction for Collection Monoids
    */
  def bag(e: Exp) = ???

  def list(e: Exp) = ???

  def set(e: Exp) = ???

  /** Unary Expressions
    */
  def not(e: Builder) = UnaryExpBuilder(Not(), e)

  def -(e: Builder) = UnaryExpBuilder(Neg(), e)

  def to_bool(e: Builder) = UnaryExpBuilder(ToBool(), e)

  def to_int(e: Builder) = UnaryExpBuilder(ToInt(), e)

  def to_float(e: Builder) = UnaryExpBuilder(ToFloat(), e)

  def to_string(e: Builder) = UnaryExpBuilder(ToString(), e)

  /** Monoids
    */
  def max = MaxMonoid()

  def multiply = MultiplyMonoid()

  def sum = SumMonoid()

  def and = AndMonoid()

  def or = OrMonoid()

  def bag = BagMonoid()

  def list = ListMonoid()

  def set = SetMonoid()

  def build(b: Builder): Exp = b match {
    case NullBuilder                     => Null
    case ConstBuilder(c)                 => c
    case ArgBuilder                      => Arg
    case RecordProjBuilder(lhs, idn)     => RecordProj(build(lhs), idn)
    case RecordConsBuilder(atts)         => RecordCons(atts.map { att => AttrCons(att.idn, build(att.b))})
    case IfThenElseBuilder(i)            => i
    case BinaryExpBuilder(op, lhs, rhs)  => BinaryExp(op, build(lhs), build(rhs))
    case MergeMonoidBuilder(m, lhs, rhs) => MergeMonoid(m, build(lhs), build(rhs))
    case UnaryExpBuilder(op, e)          => UnaryExp(op, build(e))
  }

  def argbuild(bs: List[Builder]): Exp =
    RecordCons(bs.zipWithIndex.map { case (b, idx) => AttrCons(s"_${idx + 1}", build(b)) })


  /** Algebra operators
    */
  def scan(name: String) = Scan(name, world.sources(name))

  def reduce(m: Monoid, e: Builder, p: Builder, child: LogicalAlgebraNode) = Reduce(m, build(e), build(p), child)

  def reduce(m: Monoid, e: Builder, child: LogicalAlgebraNode) = Reduce(m, build(e), BoolConst(true), child)

  def select(p: Builder, child: LogicalAlgebraNode) = Select(build(p), child)

  def select(child: LogicalAlgebraNode) = Select(BoolConst(true), child)

  def nest(m: Monoid, e: Builder, group_by: List[Builder], p: Builder, nulls: List[Builder], child: LogicalAlgebraNode) = Nest(m, build(e), argbuild(group_by), build(p), argbuild(nulls), child)

  def nest(m: Monoid, e: Builder, group_by: List[Builder], nulls: List[Builder], child: LogicalAlgebraNode) = Nest(m, build(e), argbuild(group_by), BoolConst(true), argbuild(nulls), child)

  def join(p: Builder, left: LogicalAlgebraNode, right: LogicalAlgebraNode) = Join(build(p), left, right)

  def unnest(path: Builder, pred: Builder, child: LogicalAlgebraNode) = Unnest(build(path), build(pred), child)

  def unnest(path: Builder, child: LogicalAlgebraNode) = Unnest(build(path), BoolConst(true), child)

  def outer_join(p: Builder, left: LogicalAlgebraNode, right: LogicalAlgebraNode) = OuterJoin(build(p), left, right)

  def outer_unnest(path: Builder, pred: Builder, child: LogicalAlgebraNode) = OuterUnnest(build(path), build(pred), child)

  def outer_unnest(path: Builder, child: LogicalAlgebraNode) = OuterUnnest(build(path), BoolConst(true), child)

  def merge(m: Monoid, left: LogicalAlgebraNode, right: LogicalAlgebraNode) = Merge(m, left, right)
}