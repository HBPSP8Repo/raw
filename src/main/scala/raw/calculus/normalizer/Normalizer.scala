/** The Normalization Algorithm for Monoid Comprehensions.
 *  The rules are described in [1] (Fig. 4, page 17).
 * 
 * FIXME:
 * [MSB] The application of rules in normalize() is inefficient  
 */
package raw.calculus.normalizer

import raw._
import raw.calculus._

object Normalizer {

  private def splitWith[A, B](xs: List[A], f: PartialFunction[A, B]): Option[Tuple3[List[A], B, List[A]]] = {
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
  
  private object FirstOfBind {
    def unapply(xs: List[Expression]) = splitWith[Expression, Bind](xs, { case x: Bind => x })
  }

  private object FirstOfGenerator {
    def unapply(xs: List[Expression]) = splitWith[Expression, Generator](xs, { case x: Generator => x })
  }

  private object FirstOfComprehension {
    def unapply(xs: List[Expression]) = splitWith[Expression, Comprehension](xs, { case x: Comprehension => x })
  }
  
  private def betaReduction(e: TypedExpression, x: Variable, u: TypedExpression): TypedExpression = e match {
    case n : Null => n
    case c : Constant => c
    case v : Variable if v == x => u
    case v : Variable => v
    case c : ClassExtent => c
    case RecordProjection(t, e, name) => RecordProjection(t, betaReduction(e, x, u), name)
    case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, betaReduction(att.e, x, u))))
    case IfThenElse(t, e1, e2, e3) => IfThenElse(t, betaReduction(e1, x, u), betaReduction(e2, x, u), betaReduction(e3, x, u))
    case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, betaReduction(e1, x, u), betaReduction(e2, x, u))
    case FunctionAbstraction(v, t, e) => FunctionAbstraction(v, t, betaReduction(e, x, u))
    case FunctionApplication(t, e1, e2) => FunctionApplication(t, betaReduction(e1, x, u), betaReduction(e2, x, u))
    case z : EmptySet => z
    case z : EmptyBag => z
    case z : EmptyList => z
    case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, betaReduction(e, x, u))
    case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, betaReduction(e1, x, u), betaReduction(e2, x, u))
    case Comprehension(t, m, e, qs) =>
      Comprehension(t, m, betaReduction(e, x, u), qs.map(q => q match {
        case te : TypedExpression => betaReduction(te, x, u)
        case Generator(v, e) => Generator(v, betaReduction(e, x, u))
        case Bind(v, e) => Bind(v, betaReduction(e, x, u))
      }))
  }  
  
  private def apply(e: TypedExpression): TypedExpression = e match {
  
    /** Rule 2 */
    case FunctionApplication(_, FunctionAbstraction(_, v, e1), e2) => betaReduction(e1, v, e2)
    
    /** Rule 3 */
    case RecordProjection(_, RecordConstruction(_, atts), name) => atts.collect{case att if att.name == name => att.e}.head 
  
    /** Rule 10 */
    case Comprehension(t, SumMonoid(), Comprehension(_, SumMonoid(), e, r), s) => Comprehension(t, SumMonoid(), e, s ++ r)
    case Comprehension(t, MultiplyMonoid(), Comprehension(_, MultiplyMonoid(), e, r), s) => Comprehension(t, MultiplyMonoid(), e, s ++ r)
    case Comprehension(t, MaxMonoid(), Comprehension(_, MaxMonoid(), e, r), s) => Comprehension(t, MaxMonoid(), e, s ++ r)
    case Comprehension(t, OrMonoid(), Comprehension(_, OrMonoid(), e, r), s) => Comprehension(t, OrMonoid(), e, s ++ r)
    case Comprehension(t, AndMonoid(), Comprehension(_, AndMonoid(), e, r), s) => Comprehension(t, AndMonoid(), e, s ++ r)
  
    /** Rules 1, 4, 5, 6, 7, 8, 9
     */
    case c @ Comprehension(t, m, e, qs) => qs match {
      
      /** Rule 1 */
      case FirstOfBind(q, Bind(x, u), s) => Comprehension(t, m, betaReduction(e, x, u), q ++ (s.map(se => se match {
        case te : TypedExpression => betaReduction(te, x, u)
        case Generator(v, e) => Generator(v, betaReduction(e, x, u))
        case Bind(v, e) => Bind(v, betaReduction(e, x, u))
      })))
      
      /** Rule 9 */
      case FirstOfComprehension(q, Comprehension(_, OrMonoid(), pred, r), s) if m.idempotent =>
        Comprehension(t, m, e, q ++ r ++ List(pred) ++ s)

      /** Rules 4, 5, 6, 7, 8
       */
      case FirstOfGenerator(q, Generator(v, ge), s) => ge match {
        /** Rule 4: apply() is called recursively so that variable 'v' is re-use in independent scopes */
        case IfThenElse(_, e1, e2, e3) if m.commutative || q.isEmpty => MergeMonoid(t, m,
                                            apply(Comprehension(t, m, e, q ++ List(e1, Generator(v, e2)) ++ s)),
                                            apply(Comprehension(t, m, e, q ++ List(Not(e1), Generator(v, e3)) ++ s))
                                          )
        /** Rule 5 */
        case ze : EmptySet => ze
        case ze : EmptyBag => ze
        case ze : EmptyList => ze
        
        /** Rule 6 */
        case ConsCollectionMonoid(_, _, e1) => Comprehension(t, m, e, q ++ List(Bind(v, e1)) ++ s)
        
        /** Rule 7: apply() is called recursively so that variable 'v' is re-use in independent scopes */
        case MergeMonoid(_, _, e1, e2) if m.commutative || q.isEmpty => MergeMonoid(t, m,
                                            apply(Comprehension(t, m, e, q ++ List(Generator(v, e1)) ++ s)),
                                            apply(Comprehension(t, m, e, q ++ List(Generator(v, e2)) ++ s))
                                          )                                                 
                                          
        /** Rule 8 */
        case Comprehension(_, _, e1, r) => Comprehension(t, m, e, q ++ r ++ List(Bind(v, e1)) ++ s)
        
        /** Extra rule */
        case ce : ClassExtent => c
      }
    }
  }

  def normalize(e: TypedExpression) = {
    var ne1 = apply(e)
    var ne2 = apply(ne1)
    while (ne1 != ne2) {
      ne1 = ne2
      ne2 = apply(ne1)
    }
    ne1
  }
}