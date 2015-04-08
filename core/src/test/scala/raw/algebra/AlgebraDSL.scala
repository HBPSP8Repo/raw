package raw
package algebra

case class AlgebraDSLError(err: String) extends RawException(err)

abstract class AlgebraDSL {

  import scala.language.implicitConversions
  import scala.language.dynamics
  import scala.collection.immutable.Seq
  import LogicalAlgebra._
  import Expressions._

  /** World
    */
  val world: World

  private lazy val typer = new Typer(world)

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
  }

  case object NullBuilder extends Builder

  case class ConstBuilder(c: Const) extends Builder

  case object ArgBuilder extends Builder

  case class RecordProjBuilder(lhs: Builder, idn: String) extends Builder

  case class AttrConsBuilder(idn: String, b: Builder)

  case class RecordConsBuilder(atts: Seq[AttrConsBuilder]) extends Builder

  case class IfThenElseBuilder(i: IfThenElse) extends Builder

  case class BinaryExpBuilder(op: BinaryOperator, lhs: Builder, rhs: Builder) extends Builder

  case class MergeMonoidBuilder(m: PrimitiveMonoid, lhs: Builder, rhs: Builder) extends Builder

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

  private def build(b: Builder, t: Type): Exp = {
    def recurse(b: Builder): Exp = b match {
      case NullBuilder                     => Null
      case ConstBuilder(c)                 => c
      case ArgBuilder                      => Arg(t)
      case RecordProjBuilder(lhs, idn)     => RecordProj(recurse(lhs), idn)
      case RecordConsBuilder(atts)         => RecordCons(atts.map { att => AttrCons(att.idn, recurse(att.b))})
      case IfThenElseBuilder(i)            => i
      case BinaryExpBuilder(op, lhs, rhs)  => BinaryExp(op, recurse(lhs), recurse(rhs))
      case MergeMonoidBuilder(m, lhs, rhs) => MergeMonoid(m, recurse(lhs), recurse(rhs))
      case UnaryExpBuilder(op, e)          => UnaryExp(op, recurse(e))
    }
    recurse(b)
  }

  private def tipe(a: LogicalAlgebraNode): Type =
    typer.tipe(a) match { case c: CollectionType => c.innerType }

  private def exptipe(e: Exp): Type =
    typer.expressionType(e)

  private def innerexptipe(e: Exp): Type =
    typer.expressionType(e) match { case c: CollectionType => c.innerType }

  private def rectipe(t1: Type, t2: Type): Type =
    RecordType(List(AttrType("_1", t1), AttrType("_2", t2)))

  /** Algebra operators
    */
  def scan(name: String) =
    Scan(name, world.sources(name))

  def reduce(m: Monoid, e: Builder, p: Builder, child: LogicalAlgebraNode): Reduce = {
    val t = tipe(child)
    val ne = build(e, t)
    val np = build(p, rectipe(t, exptipe(ne)))
    Reduce(m, ne, np, child)
  }

  def reduce(m: Monoid, e: Builder, child: LogicalAlgebraNode): Reduce =
    reduce(m, e, ConstBuilder(BoolConst(true)), child)

  def select(p: Builder, child: LogicalAlgebraNode): Select = {
    val np = build(p, tipe(child))
    Select(np, child)
  }

  def select(child: LogicalAlgebraNode): Select =
    select(ConstBuilder(BoolConst(true)), child)

  def nest(m: Monoid, e: Builder, group_by: Builder, p: Builder, nulls: Builder, child: LogicalAlgebraNode): Nest = {
    val t = tipe(child)
    val ne = build(e, t)
    val ngroup_by = build(group_by, t)
    val np = build(p, t)
    val nnulls = build(nulls, t)
    Nest(m, ne, ngroup_by, np, nnulls, child)
  }

  def nest(m: Monoid, e: Builder, group_by: Builder, nulls: Builder, child: LogicalAlgebraNode): Nest =
    nest(m, e, group_by, ConstBuilder(BoolConst(true)), nulls, child)

  def join(p: Builder, left: LogicalAlgebraNode, right: LogicalAlgebraNode) = {
    val t1 = tipe(left)
    val t2 = tipe(right)
    val t = rectipe(t1, t2)
    Join(build(p, t), left, right)
  }

  def unnest(path: Builder, pred: Builder, child: LogicalAlgebraNode): Unnest = {
    val t = tipe(child)
    val npath = build(path, t)
    val npred = build(pred, rectipe(t, innerexptipe(npath)))
    Unnest(npath, npred, child)
  }

  def unnest(path: Builder, child: LogicalAlgebraNode): Unnest =
    unnest(path, ConstBuilder(BoolConst(true)), child)

  def outer_join(p: Builder, left: LogicalAlgebraNode, right: LogicalAlgebraNode) = {
    val t1 = tipe(left)
    val t2 = tipe(right)
    val t = rectipe(t1, t2)
    OuterJoin(build(p, t), left, right)
  }

  def outer_unnest(path: Builder, pred: Builder, child: LogicalAlgebraNode): OuterUnnest = {
    val t = tipe(child)
    val npath = build(path, t)
    val npred = build(pred, rectipe(t, innerexptipe(npath)))
    OuterUnnest(npath, npred, child)
  }

  def outer_unnest(path: Builder, child: LogicalAlgebraNode): OuterUnnest =
    outer_unnest(path, ConstBuilder(BoolConst(true)), child)

  def merge(m: Monoid, left: LogicalAlgebraNode, right: LogicalAlgebraNode) =
    Merge(m, left, right)
}