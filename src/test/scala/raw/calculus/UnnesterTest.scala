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
    Unnester(nt)
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
                  path=arg(0).children,
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
            arg(0),
            select(
              scan("things"))),
          reduce(
            set,
            arg(0),
            select(
              scan("things"))))
      }
    }
    assert(process(TestWorlds.things, query) === Result())
  }
}


case class AlgebraDSLError(err: String) extends RawException

class AlgebraLang {

  import scala.language.implicitConversions
  import scala.language.dynamics
  import raw._
  import algebra.LogicalAlgebra._

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

  case class ArgBuilder(a: Arg) extends Builder

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

  implicit def intToExp(v: Int): ConstBuilder = ConstBuilder(IntConst(v))

  implicit def floatToExp(v: Float): ConstBuilder = ConstBuilder(FloatConst(v))

  implicit def stringToExp(v: String): ConstBuilder = ConstBuilder(StringConst(v))

  /** Variable
    * TODO: Create new algebra node "var" with i as an argument
    */
  def arg(i: Int) = ArgBuilder(Arg(i))

  /** Record Construction
    */
  def record(atts: Tuple2[String, Builder]*) = RecordConsBuilder(atts.map { att => AttrConsBuilder(att._1, att._2)})

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

  // TODO: For ease-of-use, always convert any predicate to CNF
  private def cnfToList(p: Exp): List[Exp] = p match {
    case MergeMonoid(_: AndMonoid, e1, e2) => cnfToList(e1) ++ cnfToList(e2)
    case _                                 => List(p)
  }

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

  // TODO: Distinguish between nodes that actually need a build from those that do not (e.g that have already the inner object)
  def build(b: Builder): Exp = b match {
    case NullBuilder                     => Null
    case ConstBuilder(c)                 => c
    case ArgBuilder(a)                   => a
    case RecordProjBuilder(lhs, idn)     => RecordProj(build(lhs), idn)
    case RecordConsBuilder(atts)         => RecordCons(atts.map { att => AttrCons(att.idn, build(att.b))})
    case IfThenElseBuilder(i)            => i
    case BinaryExpBuilder(op, lhs, rhs)  => BinaryExp(op, build(lhs), build(rhs))
    case MergeMonoidBuilder(m, lhs, rhs) => MergeMonoid(m, build(lhs), build(rhs))
    case UnaryExpBuilder(op, e)          => UnaryExp(op, build(e))
  }

  def argbuild(b: ArgBuilder): Arg = b match {
    case ArgBuilder(a) => a
  }

  // TODO: comment saying we reuse builder for path as well
  def builderToPath(p: Builder): Path = p match {
    case RecordProjBuilder(lhs, idn) => InnerPath(builderToPath(lhs), idn)
    case ArgBuilder(a)               => BoundArg(a)
    case _                           => throw AlgebraDSLError(s"Invalid builder in path: $p")
  }

  /** Algebra operators
    */
  def scan(name: String) = Scan(name)

  def reduce(m: Monoid, e: Builder, p: Builder, child: AlgebraNode) = Reduce(m, build(e), cnfToList(build(p)), child)

  def reduce(m: Monoid, e: Builder, child: AlgebraNode) = Reduce(m, build(e), Nil, child)

  def select(p: Builder, child: AlgebraNode) = Select(cnfToList(build(p)), child)

  def select(child: AlgebraNode) = Select(Nil, child)

  def nest(m: Monoid, e: Builder, group_by: List[ArgBuilder], p: Builder, nulls: List[ArgBuilder], child: AlgebraNode) = Nest(m, build(e), group_by.map(argbuild(_)), cnfToList(build(p)), nulls.map(argbuild(_)), child)

  def nest(m: Monoid, e: Builder, group_by: List[ArgBuilder], nulls: List[ArgBuilder], child: AlgebraNode) = Nest(m, build(e), group_by.map(argbuild(_)), Nil, nulls.map(argbuild(_)), child)

  def join(p: Builder, left: AlgebraNode, right: AlgebraNode) = Join(cnfToList(build(p)), left, right)

  def unnest(path: Builder, pred: Builder, child: AlgebraNode) = Unnest(builderToPath(path), cnfToList(build(pred)), child)

  def unnest(path: Builder, child: AlgebraNode) = Unnest(builderToPath(path), Nil, child)

  def outer_join(p: Builder, left: AlgebraNode, right: AlgebraNode) = OuterJoin(cnfToList(build(p)), left, right)

  def outer_unnest(path: Builder, pred: Builder, child: AlgebraNode) = OuterUnnest(builderToPath(path), cnfToList(build(pred)), child)

  def outer_unnest(path: Builder, child: AlgebraNode) = OuterUnnest(builderToPath(path), Nil, child)

  def merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) = Merge(m, left, right)
}
