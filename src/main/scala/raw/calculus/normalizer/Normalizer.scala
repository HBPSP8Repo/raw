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

  private def apply(e: parser.TypedExpression): parser.TypedExpression = {
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
    
    object Path {
      def unapply(e: TypedExpression): Option[TypedExpression] = e match {
        case v: Variable => Some(v)
        case RecordProjection(_, Path(_), _) => Some(e)
      }
    }
      
    def betaReduction(e: TypedExpression, x: Variable, u: TypedExpression): TypedExpression = e match {
      case n : Null => n
      case c : Constant => c
      case v : Variable if v == x => u
      case v : Variable => v
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
      case Not(e) => Not(betaReduction(e, x, u))
    }   
    
    def rewriteVariable(e: TypedExpression, v: Variable, nv: Variable): TypedExpression = e match {
      case n : Null => n
      case c : Constant => c
      case v1 : Variable if v == v1 => nv
      case v1 : Variable => v1
      case RecordProjection(t, e, name) => RecordProjection(t, rewriteVariable(e, v, nv), name)
      case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, rewriteVariable(att.e, v, nv))))
      case IfThenElse(t, e1, e2, e3) => IfThenElse(t, rewriteVariable(e1, v, nv), rewriteVariable(e2, v, nv), rewriteVariable(e3, v, nv))
      case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, rewriteVariable(e1, v, nv), rewriteVariable(e2, v, nv))
      case FunctionAbstraction(v1, t, e) => FunctionAbstraction(v1, t, rewriteVariable(e, v, nv))
      case FunctionApplication(t, e1, e2) => FunctionApplication(t, rewriteVariable(e1, v, nv), rewriteVariable(e2, v, nv))
      case z : EmptySet => z
      case z : EmptyBag => z
      case z : EmptyList => z
      case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, rewriteVariable(e, v, nv))
      case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, rewriteVariable(e1, v, nv), rewriteVariable(e2, v, nv))
      case Comprehension(t, m, e, qs) =>
        Comprehension(t, m, rewriteVariable(e, v, nv), qs.map(q => q match {
          case te : TypedExpression => rewriteVariable(te, v, nv)
          case Generator(v1, e) => Generator(v1, rewriteVariable(e, v, nv))
          case Bind(v1, e) => Bind(v1, rewriteVariable(e, v, nv))
        }))
      case Not(e) => Not(rewriteVariable(e, v, nv))      
    }
    
    e match {
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
    
      /** Rules 1, 4, 5, 6, 7, 8, 9
       */
      case c @ Comprehension(t, m, e, qs) => qs match {
        
        /** Rule 1
         */
        case FirstOfBind(q, Bind(x, u), s) => Comprehension(t, m, betaReduction(e, x, u), q ++ (s.map(se => se match {
          case te : TypedExpression => betaReduction(te, x, u)
          case Generator(v, e) => Generator(v, betaReduction(e, x, u))
          case Bind(v, e) => Bind(v, betaReduction(e, x, u))
        })))
        
        /** Rule 9
         */
        case FirstOfComprehension(q, Comprehension(_, OrMonoid(), pred, r), s) if m.idempotent =>
          Comprehension(t, m, e, q ++ r ++ List(pred) ++ s)
  
        /** Rules 4, 5, 6, 7, 8
         */
        case FirstOfGenerator(q, Generator(v, ge), s) => ge match {
          /** Rule 4
           *  
           *  Variable 'v' is replaced in both sides of the expression by a new variable so that
           *  we don't get conflicts in reusing the same variable in separate "scopes".
           */
          case IfThenElse(_, e1, e2, e3) if m.commutative || q.isEmpty => MergeMonoid(t, m,
                                              rewriteVariable(Comprehension(t, m, e, q ++ List(e1, Generator(v, e2)) ++ s), v, Variable(v.monoidType)),
                                              rewriteVariable(Comprehension(t, m, e, q ++ List(Not(e1), Generator(v, e3)) ++ s), v, Variable(v.monoidType))
                                            )
                                              
          /** Rule 5
           */
          case ze : EmptySet => ze
          case ze : EmptyBag => ze
          case ze : EmptyList => ze
          
          /** Rule 6
           */
          case ConsCollectionMonoid(_, _, e1) => Comprehension(t, m, e, q ++ List(Bind(v, e1)) ++ s)
          
          /** Rule 7
           *  
           *  Variable 'v' is replaced in both sides of the expression by a new variable so that
           *  we don't get conflicts in reusing the same variable in separate "scopes".
           */
          case MergeMonoid(_, _, e1, e2) if m.commutative || q.isEmpty => MergeMonoid(t, m,
                                              rewriteVariable(Comprehension(t, m, e, q ++ List(Generator(v, e1)) ++ s), v, Variable(v.monoidType)),
                                              rewriteVariable(Comprehension(t, m, e, q ++ List(Generator(v, e2)) ++ s), v, Variable(v.monoidType))
                                            )                                                 
                                            
          /** Rule 8
           */
          case Comprehension(_, _, e1, r) => Comprehension(t, m, e, q ++ r ++ List(Bind(v, e1)) ++ s)
          
          /** Extra rule
           */
          case Path(_) => c
        }
      }
      
      /** Extra rule for expressions that cannot be normalized further.
       */
      case _ => e
    }
  }
  
  /** This method converts a normalized expression in Parser calculus to Normalizer calculus.
   */
  def convert(e: parser.TypedExpression): TypedExpression = e match {
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
  def normalize(e: parser.TypedExpression): TypedExpression = {
    var ne1 = apply(e)
    var ne2 = apply(ne1)
    while (ne1 != ne2) {
      ne1 = ne2
      ne2 = apply(ne1)
    }
    println("(debug) Normalized expression: " + ne1)
    convert(ne1)
  }
}