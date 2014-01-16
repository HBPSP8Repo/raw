/** The Normalization Algorithm for Monoid Comprehensions.
 *  The rules are described in [1] (Fig. 4, page 17).
 */
package raw.calculus.normalizer

import raw._
import raw.calculus._

object Normalizer {

  private def normalize(e: parser.TypedExpression): parser.TypedExpression = {
    import raw.calculus.parser._
  
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
    
    object FirstOfBind {
      def unapply(xs: List[Expression]) = splitWith[Expression, Bind](xs, { case x: Bind => x })
    }
  
    object FirstOfGenerator {
      def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x: Generator => x })
    }
  
    object FirstOfComprehension {
      def unapply(xs: List[Expression]) = splitWith[Expression, Comprehension](xs, { case x: Comprehension => x })
    }
    
    def recurse(e: TypedExpression, f: PartialFunction[TypedExpression, TypedExpression]): TypedExpression =
      if (f.isDefinedAt(e))
        recurse(f(e), f)
      else
        e match {
          case n : Null => n 
          case c : Constant => c
          case v : Variable => v
          case RecordProjection(t, e, name) => RecordProjection(t, recurse(e, f), name)
          case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, recurse(att.e, f))))
          case IfThenElse(t, e1, e2, e3) => IfThenElse(t, recurse(e1, f), recurse(e2, f), recurse(e3, f))
          case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, recurse(e1, f), recurse(e2, f))
          case FunctionAbstraction(v, t, e) => FunctionAbstraction(v, t, recurse(e, f))
          case FunctionApplication(t, e1, e2) => FunctionApplication(t, recurse(e1, f), recurse(e2, f))
          case z : EmptySet => z
          case z : EmptyBag => z
          case z : EmptyList => z
          case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, recurse(e, f))
          case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, recurse(e1, f), recurse(e2, f))
          case Comprehension(t, m, e, qs) =>
            Comprehension(t, m, recurse(e, f), qs.map(q => q match {
              case te : TypedExpression => recurse(te, f)
              case Generator(v, e) => Generator(v, recurse(e, f))
              case Bind(v, e) => Bind(v, recurse(e, f))
            }))
          case Not(e) => Not(recurse(e, f))              
        }    
      
    /** Replaces variable 'x' by expression 'u' in expression 'e'.
     */
    def betaReduction(e: TypedExpression, x: Variable, u: TypedExpression): TypedExpression = 
      recurse(e, {case v : Variable if v == x => u})
    
    /** Replaces variable 'v1' by variable 'v2' in expression 'e'.
     */
    def rewriteVariable(e: TypedExpression, v1: Variable, v2: Variable): TypedExpression = 
      recurse(e, {case v : Variable if v == v1 => v2})
    
    /** Normalization Rules.
     */
    recurse(e, {
      /** Rule 2
       */
      case FunctionApplication(_, FunctionAbstraction(_, v, e1), e2) => betaReduction(e1, v, e2)
      
      /** Rule 3
       */
      case RecordProjection(_, RecordConstruction(_, atts), name) => atts.collect{case att if att.name == name => att.e}.head 
    
      /** Rule 10
       */
      case Comprehension(t, SumMonoid(), Comprehension(_, SumMonoid(), e, r), s) => Comprehension(t, SumMonoid(), e, s ++ r)
      case Comprehension(t, MultiplyMonoid(), Comprehension(_, MultiplyMonoid(), e, r), s) => Comprehension(t, MultiplyMonoid(), e, s ++ r)
      case Comprehension(t, MaxMonoid(), Comprehension(_, MaxMonoid(), e, r), s) => Comprehension(t, MaxMonoid(), e, s ++ r)
      case Comprehension(t, OrMonoid(), Comprehension(_, OrMonoid(), e, r), s) => Comprehension(t, OrMonoid(), e, s ++ r)
      case Comprehension(t, AndMonoid(), Comprehension(_, AndMonoid(), e, r), s) => Comprehension(t, AndMonoid(), e, s ++ r)

      /** Rule 1
       */
      case Comprehension(t, m, e, FirstOfBind(q, Bind(x, u), s)) => 
        Comprehension(t, m, betaReduction(e, x, u), q ++ (s.map(se => se match {
          case te : TypedExpression => betaReduction(te, x, u)
          case Generator(v, e) => Generator(v, betaReduction(e, x, u))
          case Bind(v, e) => Bind(v, betaReduction(e, x, u))
        })))

      /** Rule 9
       */
      case Comprehension(t, m, e, FirstOfComprehension(q, Comprehension(_, OrMonoid(), pred, r), s)) if m.idempotent =>
        Comprehension(t, m, e, q ++ r ++ List(pred) ++ s)
        
      /** Rule 4
       */
      case Comprehension(t, m, e, FirstOfGenerator(q, Generator(v, IfThenElse(_, e1, e2, e3)), s)) if m.commutative || q.isEmpty => 
        MergeMonoid(t, m,
          rewriteVariable(Comprehension(t, m, e, q ++ List(e1, Generator(v, e2)) ++ s), v, Variable(v.monoidType)),
          rewriteVariable(Comprehension(t, m, e, q ++ List(Not(e1), Generator(v, e3)) ++ s), v, Variable(v.monoidType))
        )
        
      /** Rule 5
       */
      case Comprehension(t, m, e, FirstOfGenerator(q, Generator(v, ze : EmptySet), s)) => ze
      case Comprehension(t, m, e, FirstOfGenerator(q, Generator(v, ze : EmptyBag), s)) => ze
      case Comprehension(t, m, e, FirstOfGenerator(q, Generator(v, ze : EmptyList), s)) => ze
        
      /** Rule 6
       */
      case Comprehension(t, m, e, FirstOfGenerator(q, Generator(v, ConsCollectionMonoid(_, _, e1)), s)) =>
        Comprehension(t, m, e, q ++ List(Bind(v, e1)) ++ s)        
      
      /** Rule 7
       */
      case Comprehension(t, m, e, FirstOfGenerator(q, Generator(v, MergeMonoid(_, _, e1, e2)), s)) if m.commutative || q.isEmpty =>
        MergeMonoid(t, m,
          rewriteVariable(Comprehension(t, m, e, q ++ List(Generator(v, e1)) ++ s), v, Variable(v.monoidType)),
          rewriteVariable(Comprehension(t, m, e, q ++ List(Generator(v, e2)) ++ s), v, Variable(v.monoidType))
        )                                      
      
      /** Rule 8
       */
      case Comprehension(t, m, e, FirstOfGenerator(q, Generator(v, Comprehension(_, _, e1, r)), s)) =>
        Comprehension(t, m, e, q ++ r ++ List(Bind(v, e1)) ++ s)        
    })
  }

  /** Convert (normalized) expression from the Parser calculus to the Normalizer calculus.
   */
  private def convert(e: parser.TypedExpression): TypedExpression = e match {
    case parser.Null() => Null()
    case parser.BoolConst(v) => BoolConst(v)
    case parser.StringConst(v) => StringConst(v)
    case parser.FloatConst(v) => FloatConst(v)
    case parser.IntConst(v) => IntConst(v)
    case v : parser.Variable => Variable(v)
    case parser.RecordProjection(t, e, name) => RecordProjection(t, convert(e), name)
    case parser.RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, convert(att.e))))
    case parser.IfThenElse(t, e1, e2, e3) => IfThenElse(t, convert(e1), convert(e2), convert(e3))
    case parser.BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, convert(e1), convert(e2))
    case f : parser.FunctionAbstraction => throw RawInternalException("unexpected FunctionAbstraction: expression not normalized")
    case f : parser.FunctionApplication => throw RawInternalException("unexpected FunctionApplication: expression not normalized")
    case parser.EmptySet() => EmptySet()
    case parser.EmptyBag() => EmptyBag()
    case parser.EmptyList() => EmptyList()
    case parser.ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, convert(e))
    case parser.MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, convert(e1), convert(e2))
    case parser.Comprehension(t, m, e, qs) => {
      val nqs = qs.map(q => q match {
        case e : parser.TypedExpression => convert(e)
        case parser.Generator(v : parser.Variable, e) => Generator(Variable(v), convert(e))
        case _ => throw RawInternalException("unexpected Bind: expression not normalized") 
      })
      val ne = convert(e)
      Comprehension(t, m, ne, nqs)
    }
    case parser.Not(e) => Not(convert(e))
  }

  /** This methods normalizes the expression written in Parser calculus and returns a new 
   *  expression written in the Normalizer calculus. The difference between the two calculus
   *  is that Bind, FunctionAbstraction and FunctionApplication do not exist in the Normalizer
   *  calculus.
   *  The normalization algorithm itself (refer to apply()) is done in Parser calculus
   *  because that is easier to express.
   *  The conversion between calculus is done after that (refer to convert()).
   */
  def apply(e: parser.TypedExpression): TypedExpression = convert(normalize(e))
}