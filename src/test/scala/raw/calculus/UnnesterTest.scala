package raw.calculus

class UnnesterTest extends FunTest {

  def process(w: World, q: String) = {
    w.unnest(parse(w, q))
  }

  test("paper_query") {
    import raw.algebra.AlgebraPrettyPrinter

    // TODO: Implement actual tests

    MyTest()

    println(AlgebraPrettyPrinter.pretty(process(TestWorlds.employees,
      """
        for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)
      """)))

    object Result extends AlgebraLang {
      def apply() = {
        reduce(
          set,
          record("E" -> arg(0), "M" -> arg(1)),
          nest(
            sum,
            e=1,
            group_by=List(arg(0)),
            p=arg(0),
            nulls=List(arg(1), arg(2)),
            nest(
              and,
              e=arg(1).age > arg(2).age,
              group_by=List(arg(0), arg(1)),
              nulls=List(arg(1)),
              outer_unnest(
                path=arg(0).manager.children,
                outer_unnest(
                  path=arg(0).children,
                  select(
                    scan("employees")))))))
      }
    }

    println("result is" + AlgebraPrettyPrinter.pretty(Result()))

    println(AlgebraPrettyPrinter.pretty(process(TestWorlds.departments,
      """for (el <- for ( d <- Departments, d.name="CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")))

    assert(false)
  }

}


class AlgebraLang {

  import scala.language.implicitConversions
  import scala.language.dynamics
  import raw.algebra._
  import raw.calculus.CanonicalCalculus._

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

  case class VarBuilder(v: Var) extends Builder

  case class RecordProjBuilder(lhs: Builder, idn: String) extends Builder

  case class AttrConsBuilder(idn: Idn, b: Builder)

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
  def arg(i: Int) = VarBuilder(Var())

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

  // TODO: Distinguish those that actually need a build from those that dont (eg that have already the inner object)
  // TODO: *** basically, c,v,r,i,e are all the same ***
  def build(b: Builder): Exp = b match {
    case NullBuilder                     => Null()
    case ConstBuilder(c)                 => c
    case VarBuilder(v)                   => v
    case RecordProjBuilder(lhs, idn)     => RecordProj(build(lhs), idn)
    case RecordConsBuilder(atts)         => RecordCons(atts.map { att => AttrCons(att.idn, build(att.b))})
    case IfThenElseBuilder(i)            => i
    case BinaryExpBuilder(op, lhs, rhs)  => BinaryExp(op, build(lhs), build(rhs))
    case MergeMonoidBuilder(m, lhs, rhs) => MergeMonoid(m, build(lhs), build(rhs))
    case UnaryExpBuilder(op, e)          => UnaryExp(op, build(e))
  }

  def varbuild(b: VarBuilder): Var = b match {
    case VarBuilder(v) => v
  }

  // TODO: comment saying we reuse builder for path as well
  def builderToPath(p: Builder): Path = p match {
    case RecordProjBuilder(lhs, idn) => InnerPath(builderToPath(lhs), idn)
    case VarBuilder(v) => BoundVar(v)
  }

  /** Algebra operators
    */
  def scan(name: String) = Scan(name)

  def reduce(m: Monoid, e: Builder, p: Builder, child: AlgebraNode) = Reduce(m, build(e), cnfToList(build(p)), child)

  def reduce(m: Monoid, e: Builder, child: AlgebraNode) = Reduce(m, build(e), Nil, child)

  def select(p: Builder, child: AlgebraNode) = Select(cnfToList(build(p)), child)

  def select(child: AlgebraNode) = Select(Nil, child)

  def nest(m: Monoid, e: Builder, group_by: List[VarBuilder], p: Builder, nulls: List[VarBuilder], child: AlgebraNode) = Nest(m, build(e), group_by.map(varbuild(_)), cnfToList(build(p)), nulls.map(varbuild(_)), child)

  def nest(m: Monoid, e: Builder, group_by: List[VarBuilder], nulls: List[VarBuilder], child: AlgebraNode) = Nest(m, build(e), group_by.map(varbuild(_)), Nil, nulls.map(varbuild(_)), child)

  def join(p: Builder, left: AlgebraNode, right: AlgebraNode) = Join(cnfToList(build(p)), left, right)

  def unnest(path: Builder, pred: Builder, child: AlgebraNode) = Unnest(builderToPath(path), cnfToList(build(pred)), child)

  def unnest(path: Builder, child: AlgebraNode) = Unnest(builderToPath(path), Nil, child)

  def outer_join(p: Builder, left: AlgebraNode, right: AlgebraNode) = OuterJoin(cnfToList(build(p)), left, right)

  def outer_unnest(path: Builder, pred: Builder, child: AlgebraNode) = OuterUnnest(builderToPath(path), cnfToList(build(pred)), child)

  def outer_unnest(path: Builder, child: AlgebraNode) = OuterUnnest(builderToPath(path), Nil, child)

  def merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) = Merge(m, left, right)
}

object MyTest extends AlgebraLang {
  def apply() = {

//    println(
//      outer_unnest(
//        arg0.children,
//        filter=arg1.age < 10,
//        select(arg0.age > arg1.age + 1,
//          scan("foo"))
//      ))

    // TODO: an issue here is that even in the tests, i can always build a bad expression
    // TODO: and also a bad reference to vars (that dont exist) and so forth...

    println(
      select(arg(0).age > arg(1).age + 1,
        scan("foo")))

    println(select(1 + 2,
      scan("employees")))

//    println(nest(sum, 1, Nil, Nil, arg(0) && arg(1),
//      outer_unnest(arg(0).children,
//        select(1 + 2,
//          scan("employees")))))
//
//
//
//
//    println(reduce(set,
//      record("E" -> arg(0), "M" -> arg(1)),
//      nest(sum, 1, Nil, Nil, arg(0) && arg(1),
//        outer_unnest(arg(0).children,
//          select(1 + 2,
//            scan("employees"))))))
    //    reduce(set,
    //      record("E" -> arg(0), "M" -> arg(1)),
    //      nest(sum, 1, Nil, Nil, arg(0) and arg(1),
    //        outer_unnest(arg(0) >> "children",
    //          select(1 + 2,
    //            scan("employees")))))
  }
}

//class AlgebraLang {
//
//  import raw.calculus.CanonicalCalculus._
//  import raw.algebra._
//
//  // TODO: For ease-of-use, always convert any predicate to CNF
//
//  private def cnfToList(p: Exp): List[Exp] = p match {
//    case MergeMonoid(_: AndMonoid, e1, e2) => cnfToList(e1) ++ cnfToList(e2)
//    case _ => List(p)
//  }
//
//  /** Monoids
//    */
//  def max = MaxMonoid()
//  def multiply = MultiplyMonoid()
//  def sum = SumMonoid()
//  def and = AndMonoid()
//  def or = OrMonoid()
//  def bag = BagMonoid()
//  def list = ListMonoid()
//  def set = SetMonoid()
//
//  /** Algebra operators
//    */
//  def scan(name: String) = Scan(name)
//  def reduce(m: Monoid, e: Exp, p: Exp, child: AlgebraNode) = Reduce(m, e, cnfToList(p), child)
//  def reduce(m: Monoid, e: Exp, child: AlgebraNode) = Reduce(m, e, Nil, child)
//  def select(p: Exp, child: AlgebraNode) = Select(cnfToList(p), child)
//  def nest(m: Monoid, e: Exp, f: List[Var], g: List[Var], p: Exp, child: AlgebraNode) = Nest(m, e, f, cnfToList(p), g, child)
//  def join(p: Exp, left: AlgebraNode, right: AlgebraNode) = Join(cnfToList(p), left, right)
//  def unnest(path: Path, pred: Exp, child: AlgebraNode) = Unnest(path, cnfToList(pred), child)
//  def unnest(path: Path, child: AlgebraNode) = Unnest(path, Nil, child)
//  def outer_join(p: Exp, left: AlgebraNode, right: AlgebraNode) = OuterJoin(cnfToList(p), left, right)
//  def outer_unnest(path: Path, pred: Exp, child: AlgebraNode) = OuterUnnest(path, cnfToList(pred), child)
//  def outer_unnest(path: Path, child: AlgebraNode) = OuterUnnest(path, Nil, child)
//  def merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) = Merge(m, left, right)
//
//  /** Constants
//    */
//  def nul() = Null()
//  implicit def boolToExp(v: Boolean) = BoolConst(v)
//  implicit def intToExp(v: Int) = IntConst(v)
//  implicit def floatToExp(v: Float): Exp = FloatConst(v)
//  implicit def stringToExp(v: String): Exp = IntConst(v)
//
//  /** Variable
//    * TODO: Create new algebra node "var" with i as an argument
//    */
//  def arg(i: Int) = Var()
//
//  /** Record Projection
//    */
//  implicit class >>(e: Exp) {
//    def apply(idn: String) = RecordProj(e, idn)
//  }
//
//  /** Record Construction
//    */
//  def record(atts: Tuple2[String, Exp]*) = RecordCons(atts.map{att => AttrCons(att._1, att._2)})
//
//  /** If `e1` Then `e2` Else `e3`
//    */
//  case class ThenElse(e2: Exp, e3: Exp)
//  def ?(e1: Exp, thenElse: ThenElse) = IfThenElse(e1, thenElse.e2, thenElse.e3)
//  def unary_:(e2: Exp, e3: Exp) = ThenElse(e2, e3)
//
//  /** Binary Expressions
//    */
//  def ==(e1: Exp, e2: Exp) = BinaryExp(Eq(), e1, e2)
//  def <>(e1: Exp, e2: Exp) = BinaryExp(Neq(), e1, e2)
//  def >=(e1: Exp, e2: Exp) = BinaryExp(Ge(), e1, e2)
//  def >(e1: Exp, e2: Exp) = BinaryExp(Ge(), e1, e2)
//  def <=(e1: Exp, e2: Exp) = BinaryExp(Le(), e1, e2)
//  def <(e1: Exp, e2: Exp) = BinaryExp(Lt(), e1, e2)
//  def -(e1: Exp, e2: Exp) = BinaryExp(Sub(), e1, e2)
//  def /(e1: Exp, e2: Exp) = BinaryExp(Div(), e1, e2)
//
//  /** Zero and Construction for Collection Monoids
//    */
//  def bag(es: Exp*) =
//    if (es.isEmpty)
//      ZeroCollectionMonoid(BagMonoid())
//    else
//      ???
//
//  def list(es: Exp*) = ???
//  def set(es: Exp*) = ???
//
//  /** Merge Monoids
//    */
//  def max(lhs: Exp, rhs: Exp): Exp = MergeMonoid(MaxMonoid(), lhs, rhs)
//  def *(lhs: Exp, rhs: Exp): Exp = MergeMonoid(MultiplyMonoid(), lhs, rhs)
//  def +(lhs: Exp, rhs: Exp): Exp = MergeMonoid(SumMonoid(), lhs, rhs)
//  implicit class and(lhs: Exp) {
//    def apply(rhs: Exp): Exp = MergeMonoid(AndMonoid(), lhs, rhs)
//  }
//  def or(lhs: Exp, rhs: Exp): Exp = MergeMonoid(OrMonoid(), lhs, rhs)
//  def bag(lhs: Exp, rhs: Exp): Exp = MergeMonoid(BagMonoid(), lhs, rhs)
//  def list(lhs: Exp, rhs: Exp): Exp = MergeMonoid(ListMonoid(), lhs, rhs)
//  def set(lhs: Exp, rhs: Exp): Exp = MergeMonoid(SetMonoid(), lhs, rhs)
//
//  /** Unary Expressions
//    */
//  def not(e: Exp) = UnaryExp(Not(), e)
//  def -(e: Exp) = UnaryExp(Neg(), e)
//  def to_bool(e: Exp) = UnaryExp(ToBool(), e)
//  def to_int(e: Exp) = UnaryExp(ToInt(), e)
//  def to_float(e: Exp) = UnaryExp(ToFloat(), e)
//  def to_string(e: Exp) = UnaryExp(ToString(), e)
//
//
//}
//
//object MyTest extends AlgebraLang {
//  def apply() = {
//    reduce(set,
//      record("E" -> arg(0), "M" -> arg(1)),
//      nest(sum, 1, Nil, Nil, arg(0) and arg(1),
//        outer_unnest(arg(0) >> "children",
//          select(1 + 2,
//            scan("employees")))))
//  }
//}