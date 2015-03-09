package raw.calculus

import raw.{RawException, World}
import raw.algebra._

class UnnesterTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new SemanticAnalyzer(t, w)
    assert(analyzer.errors.length === 0)
    val nt = Simplifier(t, w)
    Unnester(nt, w)
  }

  test("simple_join") {
    val query = "for (a <- Departments, b <- Departments, a.dno = b.dno) yield set (a1 := a, b1 := b)"

    object Result extends AlgebraLang {
      def apply() = {
        reduce(
          set,
          record("a1" -> arg(0), "b1" -> arg(1)),
          join(
            arg(0).dno == arg(1).dno,
            select(scan("Departments")),
            select(scan("Departments"))))
      }
    }

    assert(process(TestWorlds.departments, query) === Result())
  }

  test("complex_join") {
    val query = "for (speed_limit <- speed_limits, observation <- radar, speed_limit.location = observation.location, observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)"

    object Result extends AlgebraLang {
      def apply() = {
        reduce(
          list,
          record("name" -> arg(1).person, "location" -> arg(1).location),
          join(
            arg(0).location == arg(1).location && arg(1).speed > arg(0).max_speed,
            select(scan("speed_limits")),
            select(scan("radar"))))
      }
    }

    assert(process(TestWorlds.fines, query) === Result())
  }

  test("complex_join_2") {
    val query = "for (speed_limit <- speed_limits, observation <- radar, speed_limit.location = observation.location, observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)"
//
//    object Result extends AlgebraLang {
//      def apply() = {
//
//      }
//    }
//
//    println(AlgebraPrettyPrinter.pretty(process(TestWorlds.fines, query)))

    assert(false)
  }

  test("paper_query") {
    val query = "for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)"

    object Result extends AlgebraLang {
      def apply() = {
        reduce(
          set,
          record("E" -> arg(0), "M" -> arg(1)),
          nest(
            sum,
            e=1,
            group_by=List(arg(0)),
            p=arg(2),
            nulls=List(arg(1), arg(2)),
            nest(
              and,
              e=arg(1).age > arg(2).age,
              group_by=List(arg(0), arg(1)),
              nulls=List(arg(2)),
              outer_unnest(
                path=arg(0).manager.children,
                outer_unnest(
                  path=arg().children,
                  select(
                    scan("Employees")))))))
      }
    }

    assert(process(TestWorlds.employees, query) === Result())
  }

  test("top_level_merge") {
    val query = "for (x <- things union things) yield set x"

    object Result extends AlgebraLang {
      def apply() = {
        merge(
          set,
          reduce(
            set,
            arg(),
            select(
              scan("things"))),
          reduce(
            set,
            arg(),
            select(
              scan("things"))))
      }
    }
    assert(process(TestWorlds.things, query) === Result())
  }

}


case class AlgebraDSLError(err: String) extends RawException(err)

class AlgebraLang {

  import scala.language.implicitConversions
  import scala.language.dynamics
  import scala.collection.immutable.Seq
  import raw._
  import algebra.LogicalAlgebra._
  import algebra.Expressions._

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

  case class ProductProjBuilder(lhs: Builder, idx: Int) extends Builder

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
  def arg(i: Int = -1) = if (i < 0) ArgBuilder else ProductProjBuilder(ArgBuilder, i)

  /** Record Construction
    */
  def record(atts: Tuple2[String, Builder]*) = RecordConsBuilder(atts.map{ att => AttrConsBuilder(att._1, att._2)}.to[scala.collection.immutable.Seq])

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
    case ProductProjBuilder(lhs, idx)    => ProductProj(build(lhs), idx)
    case RecordProjBuilder(lhs, idn)     => RecordProj(build(lhs), idn)
    case RecordConsBuilder(atts)         => RecordCons(atts.map { att => AttrCons(att.idn, build(att.b))})
    case IfThenElseBuilder(i)            => i
    case BinaryExpBuilder(op, lhs, rhs)  => BinaryExp(op, build(lhs), build(rhs))
    case MergeMonoidBuilder(m, lhs, rhs) => MergeMonoid(m, build(lhs), build(rhs))
    case UnaryExpBuilder(op, e)          => UnaryExp(op, build(e))
  }

  def argbuild(bs: List[Builder]): Exp =
    ProductCons(bs.map(build))


  /** Algebra operators
    */
  def scan(name: String) = Scan(name)

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