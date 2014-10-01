/**
 * ;d The Normalization Algorithm for Monoid Comprehensions.
 *
 *  The rules are described in [1] (Fig. 4, page 17).
 */
package raw.calculus.normalizer

import raw._
import raw.calculus._

object Normalizer {

  private def normalize(e: parser.TypedExpression): parser.TypedExpression = {
    import raw.calculus.parser._

    /**
     * Splits a list using a partial function.
     */
    def splitWith[A, B](xs: List[A], f: PartialFunction[A, B]): Option[Tuple3[List[A], B, List[A]]] = {
      val begin = xs.takeWhile(x => if (!f.isDefinedAt(x)) true else false)
      if (begin.length == xs.length) {
        None
      } else {
        val elem = f(xs(begin.length))
        if (begin.length + 1 == xs.length) {
          Some((begin, elem, List()))
        } else {
          Some(begin, elem, xs.takeRight(xs.length - begin.length - 1))
        }
      }
    }

    object Rule1Bind {
      def unapply(xs: List[Expression]) = splitWith[Expression, Bind](xs, { case x: Bind => x })
    }

    object Rule9Comprehension {
      def unapply(xs: List[Expression]) = splitWith[Expression, Comprehension](xs, { case x @ Comprehension(_, _: OrMonoid, _, _) => x })
    }

    object Rule4Generator {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x @ Generator(_, _: IfThenElse) => x })
    }

    object Rule5GeneratorEmptySet {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x @ Generator(_, _: EmptySet) => x })
    }

    object Rule5GeneratorEmptyBag {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x @ Generator(_, _: EmptyBag) => x })
    }

    object Rule5GeneratorEmptyList {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x @ Generator(_, _: EmptyList) => x })
    }

    object Rule6Generator {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x @ Generator(_, _: ConsCollectionMonoid) => x })
    }

    object Rule7Generator {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x @ Generator(_, _: MergeMonoid) => x })
    }

    object Rule8Generator {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x @ Generator(_, _: Comprehension) => x })
    }

    def recurse(e: TypedExpression, f: PartialFunction[TypedExpression, TypedExpression]): TypedExpression =
      if (f.isDefinedAt(e))
        recurse(f(e), f)
      else
        e match {
          case n: Null => n
          case c: Constant => c
          case v: Variable => v
          case RecordProjection(t, e, name) => RecordProjection(t, recurse(e, f), name)
          case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, recurse(att.e, f))))
          case IfThenElse(t, e1, e2, e3) => IfThenElse(t, recurse(e1, f), recurse(e2, f), recurse(e3, f))
          case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, recurse(e1, f), recurse(e2, f))
          case FunctionAbstraction(v, t, e) => FunctionAbstraction(v, t, recurse(e, f))
          case FunctionApplication(t, e1, e2) => FunctionApplication(t, recurse(e1, f), recurse(e2, f))
          case z: EmptySet => z
          case z: EmptyBag => z
          case z: EmptyList => z
          case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, recurse(e, f))
          case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, recurse(e1, f), recurse(e2, f))
          case Comprehension(t, m, e, qs) =>
            Comprehension(t, m, recurse(e, f), qs.map(_ match {
              case te: TypedExpression => recurse(te, f)
              case Generator(v, e) => Generator(recurse(v, f).asInstanceOf[Variable], recurse(e, f))
              case Bind(v, e) => Bind(recurse(v, f).asInstanceOf[Variable], recurse(e, f))
            }))
          case Not(e) => Not(recurse(e, f))
          case FloatToInt(e) => FloatToInt(recurse(e, f))
          case FloatToString(e) => FloatToString(recurse(e, f))
          case IntToFloat(e) => IntToFloat(recurse(e, f))
          case IntToString(e) => IntToString(recurse(e, f))
          case StringToBool(e) => StringToBool(recurse(e, f))
          case StringToInt(e) => StringToInt(recurse(e, f))
          case StringToFloat(e) => StringToFloat(recurse(e, f))
        }

    /**
     * Replaces variable 'x' by expression 'u' in expression 'e'.
     */
    def betaReduction(e: TypedExpression, x: Variable, u: TypedExpression): TypedExpression =
      recurse(e, { case v: Variable if v == x => u })

    /**
     * Replaces variable 'v1' by variable 'v2' in expression 'e'.
     */
    def rewriteVariable(e: TypedExpression, v1: Variable, v2: Variable): TypedExpression =
      recurse(e, { case v: Variable if v == v1 => v2 })

    /**
     * Normalization Rules.
     */
    recurse(e, {
      /**
       * Rule 2
       */
      case FunctionApplication(_, FunctionAbstraction(_, v, e1), e2) => betaReduction(e1, v, e2)

      /**
       * Rule 3
       */
      case RecordProjection(_, RecordConstruction(_, atts), name) => atts.collect { case att if att.name == name => att.e }.head

      /**
       * Rule 10
       */
      case Comprehension(t, SumMonoid(), Comprehension(_, SumMonoid(), e, r), s) => Comprehension(t, SumMonoid(), e, s ++ r)
      case Comprehension(t, MultiplyMonoid(), Comprehension(_, MultiplyMonoid(), e, r), s) => Comprehension(t, MultiplyMonoid(), e, s ++ r)
      case Comprehension(t, MaxMonoid(), Comprehension(_, MaxMonoid(), e, r), s) => Comprehension(t, MaxMonoid(), e, s ++ r)
      case Comprehension(t, OrMonoid(), Comprehension(_, OrMonoid(), e, r), s) => Comprehension(t, OrMonoid(), e, s ++ r)
      case Comprehension(t, AndMonoid(), Comprehension(_, AndMonoid(), e, r), s) => Comprehension(t, AndMonoid(), e, s ++ r)

      /**
       * Rule 1
       */
      case Comprehension(t, m, e, Rule1Bind(q, Bind(x, u), s)) =>
        Comprehension(t, m, betaReduction(e, x, u), q ++ (s.map(_ match {
          case te: TypedExpression => betaReduction(te, x, u)
          case Generator(v, e) => Generator(v, betaReduction(e, x, u))
          case Bind(v, e) => Bind(v, betaReduction(e, x, u))
        })))

      /**
       * Rule 9
       */
      case Comprehension(t, m, e, Rule9Comprehension(q, Comprehension(_, OrMonoid(), pred, r), s)) if m.idempotent =>
        Comprehension(t, m, e, q ++ r ++ List(pred) ++ s)

      /**
       * Rule 4
       */
      case Comprehension(t, m, e, Rule4Generator(q, Generator(v, IfThenElse(_, e1, e2, e3)), s)) if m.commutative || q.isEmpty =>
        MergeMonoid(t, m,
          rewriteVariable(Comprehension(t, m, e, q ++ List(e1, Generator(v, e2)) ++ s), v, Variable(v.parserType, v.name)),
          rewriteVariable(Comprehension(t, m, e, q ++ List(Not(e1), Generator(v, e3)) ++ s), v, Variable(v.parserType, v.name)))

      /**
       * Rule 5
       */
      case Comprehension(t, m, e, Rule5GeneratorEmptySet(q, Generator(v, ze: EmptySet), s)) => ze
      case Comprehension(t, m, e, Rule5GeneratorEmptyBag(q, Generator(v, ze: EmptyBag), s)) => ze
      case Comprehension(t, m, e, Rule5GeneratorEmptyList(q, Generator(v, ze: EmptyList), s)) => ze

      /**
       * Rule 6
       */
      case Comprehension(t, m, e, Rule6Generator(q, Generator(v, ConsCollectionMonoid(_, _, e1)), s)) =>
        Comprehension(t, m, e, q ++ List(Bind(v, e1)) ++ s)

      /**
       * Rule 7
       */
      case Comprehension(t, m, e, Rule7Generator(q, Generator(v, MergeMonoid(_, _, e1, e2)), s)) if m.commutative || q.isEmpty =>
        MergeMonoid(t, m,
          rewriteVariable(Comprehension(t, m, e, q ++ List(Generator(v, e1)) ++ s), v, Variable(v.parserType, v.name)),
          rewriteVariable(Comprehension(t, m, e, q ++ List(Generator(v, e2)) ++ s), v, Variable(v.parserType, v.name)))

      /**
       * Rule 8
       */
      case Comprehension(t, m, e, Rule8Generator(q, Generator(v, Comprehension(_, _, e1, r)), s)) =>
        Comprehension(t, m, e, q ++ r ++ List(Bind(v, e1)) ++ s)
    })
  }

  /**
   * Convert (normalized) expression from the Parser calculus to the Normalizer calculus.
   */
  private def convert(e: parser.TypedExpression): TypedExpression = {
    def convertType(t: parser.ParserType): MonoidType = t match {
      case parser.BoolType => BoolType
      case parser.StringType => StringType
      case parser.FloatType => FloatType
      case parser.IntType => IntType
      case parser.RecordType(atts) => RecordType(atts.map(att => Attribute(att.name, convertType(att.parserType))))
      case parser.SetType(t) => SetType(convertType(t))
      case parser.BagType(t) => BagType(convertType(t))
      case parser.ListType(t) => ListType(convertType(t))
      case _ : parser.VariableType => VariableType
    }

    def convertVariable(v: parser.Variable) = Variable(convertType(v.parserType), v.name)

    e match {
      case parser.Null() => Null
      case parser.BoolConst(v) => BoolConst(v)
      case parser.StringConst(v) => StringConst(v)
      case parser.FloatConst(v) => FloatConst(v)
      case parser.IntConst(v) => IntConst(v)
      case v: parser.Variable => convertVariable(v)
      case parser.RecordProjection(t, e, name) => RecordProjection(convertType(t), convert(e), name)
      case parser.RecordConstruction(t, atts) => RecordConstruction(convertType(t), atts.map(att => AttributeConstruction(att.name, convert(att.e))))
      case parser.IfThenElse(t, e1, e2, e3) => IfThenElse(convertType(t), convert(e1), convert(e2), convert(e3))
      case parser.BinaryOperation(t, op, e1, e2) => BinaryOperation(convertType(t), op, convert(e1), convert(e2))
      case f: parser.FunctionAbstraction => throw RawInternalException("unexpected FunctionAbstraction: expression not normalized")
      case f: parser.FunctionApplication => throw RawInternalException("unexpected FunctionApplication: expression not normalized")
      case parser.EmptySet() => EmptySet
      case parser.EmptyBag() => EmptyBag
      case parser.EmptyList() => EmptyList
      case parser.ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(convertType(t), m, convert(e))
      case parser.MergeMonoid(t, m, e1, e2) => MergeMonoid(convertType(t), m, convert(e1), convert(e2))
      case parser.Comprehension(t, m, e, qs) => {
        val nqs = qs.map(q => q match {
          case e: parser.TypedExpression => convert(e)
          case parser.Generator(v: parser.Variable, e) => Generator(convertVariable(v), convert(e))
          case _ => throw RawInternalException("unexpected Bind: expression not normalized")
        })
        val ne = convert(e)
        Comprehension(convertType(t), m, ne, nqs)
      }
      case parser.Not(e) => Not(convert(e))
      case parser.FloatToInt(e) => FloatToInt(convert(e))
      case parser.FloatToString(e) => FloatToString(convert(e))
      case parser.IntToFloat(e) => IntToFloat(convert(e))
      case parser.IntToString(e) => IntToString(convert(e))
      case parser.StringToBool(e) => StringToBool(convert(e))
      case parser.StringToInt(e) => StringToInt(convert(e))
      case parser.StringToFloat(e) => StringToFloat(convert(e))
    }
  }

  /**
   * This methods normalizes a (Parser) expression and returns a new (Normalizer) expression.
   */
  def apply(e: parser.TypedExpression): TypedExpression = convert(normalize(e))
}