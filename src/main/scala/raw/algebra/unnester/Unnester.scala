package raw.algebra.unnester

import raw._
import raw.calculus._

object Unnester {

  private sealed abstract class Term
  private case class CalculusTerm(t: canonical.TypedExpression) extends Term
  private case class AlgebraTerm(t: algebra.Algebra) extends Term
  
  private sealed abstract class Pattern
  private case object EmptyPattern extends Pattern
  private case class VariablePattern(v: algebra.Variable) extends Pattern
  private case class PairPattern(a: Pattern, b: Pattern) extends Pattern
  
  /** Helper to pattern match nested comprehensions. */
  private object NestedComprehension {
    import raw.calculus.canonical._
    
    def unapply(e: Expression): Option[Comprehension] = e match {
      case RecordProjection(_, NestedComprehension(e), _) => Some(e)
      case RecordConstruction(t, atts) => atts.map(att => att.e match {
        case NestedComprehension(e) => Some(e)
        case _ => None
      }).flatten.headOption
      case IfThenElse(_, NestedComprehension(e1), _, _) => Some(e1)
      case IfThenElse(_, _, NestedComprehension(e2), _) => Some(e2)
      case IfThenElse(_, _, _, NestedComprehension(e3)) => Some(e3)
      case BinaryOperation(_, _, NestedComprehension(e1), _) => Some(e1)
      case BinaryOperation(_, _, _, NestedComprehension(e2)) => Some(e2)
      case ConsCollectionMonoid(_, _, NestedComprehension(e)) => Some(e)
      case MergeMonoid(_, _, NestedComprehension(e1), _) => Some(e1)
      case MergeMonoid(_, _, _, NestedComprehension(e2)) => Some(e2)
      case Not(NestedComprehension(e)) => Some(e)
      case c : Comprehension => Some(c)
      case _ => None
    }
  }
  
  /** This method returns true if the expression has a nested comprehension. */
  private def hasNestedComprehension(e: canonical.TypedExpression): Boolean = e match {
    case NestedComprehension(_) => true
    case _ => false
  }
  
  /** This method returns true if the comprehension 'c' does not depend on variables defined
   *  in 's' generators. */
  private def isIndependent(s: List[canonical.Generator], c: canonical.Comprehension): Boolean = {
    import raw.calculus.canonical._
    
    def apply(e: TypedExpression, gens: Set[Variable]): Boolean = e match {
      case n : Null => true
      case c : Constant => true
      case v : Variable => !gens.contains(v)
      case c : ClassExtent => true
      case RecordProjection(_, e, _) => apply(e, gens)
      case RecordConstruction(_, atts) => !atts.map(att => apply(att.e, gens)).contains(false)
      case IfThenElse(_, e1, e2, e3) => apply(e1, gens) && apply(e2, gens) && apply(e3, gens)
      case BinaryOperation(_, _, e1, e2) => apply(e1, gens) && apply(e2, gens)
      case z : EmptySet => true
      case z : EmptyBag => true
      case z : EmptyList => true
      case ConsCollectionMonoid(_, _, e) => apply(e, gens)
      case MergeMonoid(_, _, e1, e2) => apply(e1, gens) && apply(e2, gens)
      case Comprehension(_, _, e, qs, pred) => {
        !qs.collect{ case Generator(v1, e1) => apply(e1, gens) }.contains(false) && apply(pred, gens) && apply(e, gens)
      }
      case Not(e) => apply(e, gens)
    }
    
    val ss = s.collect{ case Generator(v, e) => v}.toSet
    apply(c, ss)    
  }
  
  /** This method returns a new expression with expression 've' replaced by variable 'v'. */
  private def applyVariable(e: canonical.TypedExpression, v: canonical.Variable, ve: canonical.TypedExpression): canonical.TypedExpression = {
    import raw.calculus.canonical._
    
    if (e == ve)
      v
    else {
      e match {
        case n : Null => n
        case c : Constant => c
        case v : Variable => v
        case c : ClassExtent => c
        case RecordProjection(t, e, name) => RecordProjection(t, applyVariable(e, v, ve), name)
        case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, applyVariable(att.e, v, ve))))
        case IfThenElse(t, e1, e2, e3) => IfThenElse(t, applyVariable(e1, v, ve), applyVariable(e2, v, ve), applyVariable(e3, v, ve))
        case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, applyVariable(e1, v, ve), applyVariable(e2, v, ve))
        case z : EmptySet => z
        case z : EmptyBag => z
        case z : EmptyList => z
        case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, applyVariable(e, v, ve))
        case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, applyVariable(e1, v, ve), applyVariable(e2, v, ve))
        case Comprehension(t, m, e, qs, pred) =>
          Comprehension(t, m, applyVariable(e, v, ve),
                        qs.map(q => q match { case Generator(v1, e1) => Generator(v1, applyVariable(e1, v, ve)) }),
                        applyVariable(pred, v, ve))
        case Not(e) => Not(applyVariable(e, v, ve))
      }
    }
  }
 
  /** This method returns the set of algebra variables in the pattern. */
  private def getPatternVariables(p: Pattern): Set[algebra.Variable] = p match {
    case EmptyPattern => Set()
    case VariablePattern(v) => Set(v)
    case PairPattern(a, b) => getPatternVariables(a) ++ getPatternVariables(b)
  }

  /** This method returns the set of canonical variables in the pattern. */
  private def getPatternCanonicalVariables(p: Pattern) = getPatternVariables(p).collect{ case algebra.Variable(v) => v }
  
  /** This method returns the set of variables in the expression. */
  private def getExpressionVariables(e: canonical.TypedExpression): Set[canonical.Variable] = {
    import raw.calculus.canonical._
    
    e match {
      case n : Null => Set()
      case c : Constant => Set()
      case v : Variable => Set(v)
      case c : ClassExtent => Set()
      case RecordProjection(_, e, _) => getExpressionVariables(e)
      case RecordConstruction(t, atts) => atts.flatMap(att => getExpressionVariables(att.e)).toSet
      case IfThenElse(_, e1, e2, e3) => getExpressionVariables(e1) ++ getExpressionVariables(e2) ++ getExpressionVariables(e3) 
      case BinaryOperation(_, _, e1, e2) => getExpressionVariables(e1) ++ getExpressionVariables(e2)
      case z : EmptySet => Set()
      case z : EmptyBag => Set()
      case z : EmptyList => Set()
      case ConsCollectionMonoid(_, _, e) => getExpressionVariables(e)
      case MergeMonoid(_, _, e1, e2) => getExpressionVariables(e1) ++ getExpressionVariables(e2)
      case Comprehension(_, _, e, qs, pred) =>
        getExpressionVariables(e) ++ qs.flatMap(q => q match { case Generator(_, e) => getExpressionVariables(e)}).toSet ++ getExpressionVariables(pred)
      case Not(e) => getExpressionVariables(e)
    }
  }
  
  /** Implementation of the 'split_predicate(p, left, right)' method described in page 36 of [1].
    */
  private def splitPredicate(p: canonical.TypedExpression, left: Pattern, right: Pattern) = {
    /** This method flattens a predicate in CNF form into a list of predicates. */
    def flatten(e: canonical.TypedExpression): List[canonical.TypedExpression] = e match {
       case canonical.MergeMonoid(t, AndMonoid(), e1, e2) => flatten(e1) ::: flatten(e2)
       case _ => List(e)
     }
    
    /** This method folds a list of CNF predicates into a single predicate. */
    def fold(e: List[canonical.TypedExpression]) = {
      if (e.isEmpty) {
        canonical.BoolConst(true)
      } else {
        val head = e.head
        val rest = e.drop(1)
        rest.foldLeft(head)((a, b) => canonical.MergeMonoid(BoolType, AndMonoid(), a, b))
      }
    }
        
    val pred = flatten(p) 
    val p1 = pred.filter(p => !hasNestedComprehension(p) && getExpressionVariables(p).subsetOf(getPatternCanonicalVariables(right)))
    val p2 = pred.filter(p => !hasNestedComprehension(p) && getExpressionVariables(p).subsetOf(getPatternCanonicalVariables(left)) && getExpressionVariables(p).subsetOf(getPatternCanonicalVariables(right)))
    val p3 = pred.filter(p => !p1.contains(p) && !p2.contains(p))
    (fold(p1), fold(p2), fold(p3))
  }
   
  /** This method converts a canonical typed expression to the equivalent algebra expression.
   *  The difference between the two is that Comprehension() no longer exist since the algebra
   *  operators handle the un-nesting. 
   */
  private def convert(e: canonical.TypedExpression): algebra.Expression = e match {
    case canonical.Null() => algebra.Null
    case canonical.BoolConst(v) => algebra.BoolConst(v)
    case canonical.IntConst(v) => algebra.IntConst(v)
    case canonical.FloatConst(v) => algebra.FloatConst(v)
    case canonical.StringConst(v) => algebra.StringConst(v)
    case v : canonical.Variable => algebra.Variable(v)
    case canonical.ClassExtent(t, id) => algebra.ClassExtent(t, id)
    case canonical.RecordProjection(t, e, name) => algebra.RecordProjection(t, convert(e), name)
    case canonical.RecordConstruction(t, atts) => algebra.RecordConstruction(t, atts.map(att => algebra.AttributeConstruction(att.name, convert(att.e))))
    case canonical.IfThenElse(t, e1, e2, e3) => algebra.IfThenElse(t, convert(e1), convert(e2), convert(e3))
    case canonical.BinaryOperation(t, op, e1, e2) => algebra.BinaryOperation(t, op, convert(e1), convert(e2))
    case canonical.EmptySet() => algebra.EmptySet()
    case canonical.EmptyBag() => algebra.EmptyBag()
    case canonical.EmptyList() => algebra.EmptyList()
    case canonical.ConsCollectionMonoid(t, m, e) => algebra.ConsCollectionMonoid(t, m, convert(e))
    case canonical.MergeMonoid(t, m, e1, e2) => algebra.MergeMonoid(t, m, convert(e1), convert(e2))
    case c : canonical.Comprehension => throw RawInternalException("unexpected Comprehension: expression not converted into algebra")
    case canonical.Not(e) => algebra.Not(convert(e))    
  } 
  
  /** Implementation of the Query Unnesting Algorithm described in Figure 11, page 37 of [1].
    */
  private def T(e: Term, u: Pattern, w: Pattern, E: Term): Term = e match {
    case CalculusTerm(canonical.Comprehension(t, m, e1, s, p)) => p match {
      case NestedComprehension(ep) if isIndependent(s, ep) => {
        /** Rule C11 */
        val v = canonical.Variable(normalizer.Variable(parser.Variable(ep.monoidType)))
        T(CalculusTerm(canonical.Comprehension(t, m, e1, s, applyVariable(p, v, ep))),
                       u, PairPattern(w, VariablePattern(algebra.Variable(v))),
                       T(CalculusTerm(ep), w, w, E))
      }
      case _ => e match {
        case CalculusTerm(canonical.Comprehension(t, m, e1, List(), p)) => e1 match {
           case NestedComprehension(ep) => {
             /** Rule C12 */
             val v = canonical.Variable(normalizer.Variable(parser.Variable(ep.monoidType)))
             T(CalculusTerm(canonical.Comprehension(t, m, applyVariable(e1, v, ep), List(), p)),
                            u, PairPattern(w, VariablePattern(algebra.Variable(v))),
                            T(CalculusTerm(ep), w, w, E))
           }
           case _ => {
             if (u == EmptyPattern) {
               /** Rule C5 */
               AlgebraTerm(algebra.Reduce(m,
                                          algebra.Function(getPatternVariables(w), convert(e1)),
                                          algebra.Function(getPatternVariables(w), convert(p)),
                                          E match { case AlgebraTerm(t) => t }))
             } else {
               /** Rule C8 */
               AlgebraTerm(algebra.Nest(m,
                                        algebra.Function(getPatternVariables(w), convert(e1)),
                                        algebra.FunctionVars(getPatternVariables(w), getPatternVariables(u)),
                                        algebra.Function(getPatternVariables(w), convert(p)),
                                        algebra.FunctionVars(getPatternVariables(w), getPatternVariables(w) -- getPatternVariables(u)),
                                        E match { case AlgebraTerm(t) => t }))
             }
           }
        }
        case _ => e match {
          case CalculusTerm(canonical.Comprehension(t, m, e1, canonical.Generator(v, x) :: r, p)) => {
            val (p1, p2, p3) = splitPredicate(p, w, VariablePattern(algebra.Variable(v)))
            if (u == EmptyPattern) {
              if (w == EmptyPattern) {
                /** Rule C4 */
                T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                               u, VariablePattern(algebra.Variable(v)),
                               AlgebraTerm(algebra.Selection(algebra.Function(getPatternVariables(VariablePattern(algebra.Variable(v))), convert(canonical.MergeMonoid(BoolType, AndMonoid(), p1, p2))),
                                                             convert(x))))
              } else {
                x match {
                  case canonical.Variable(_) => {
                    /** Rule C6 */
                    T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                   u, PairPattern(w, VariablePattern(algebra.Variable(v))),
                                   AlgebraTerm(algebra.Join(algebra.Function(getPatternVariables(PairPattern(w, VariablePattern(algebra.Variable(v)))), convert(p2)),
                                                            E match { case AlgebraTerm(t) => t },
                                                            algebra.Selection(algebra.Function(getPatternVariables(VariablePattern(algebra.Variable(v))), convert(p1)),
                                                                              convert(x)))))
                  }
                  case _ => {
                    /** Rule C7 */
                    T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                   u, PairPattern(w, VariablePattern(algebra.Variable(v))),
                                   AlgebraTerm(algebra.Unnest(algebra.Function(getPatternVariables(w), convert(x)),
                                                              algebra.Function(getPatternVariables(PairPattern(w, VariablePattern(algebra.Variable(v)))), convert(canonical.MergeMonoid(BoolType, AndMonoid(), p1, p2))),
                                                              E match { case AlgebraTerm(t) => t })))
                  }
                }
              }
            } else {
              x match {
                case canonical.Variable(_) => {
                  /** Rule C9 */
                  T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                 u, PairPattern(w, VariablePattern(algebra.Variable(v))),
                                 AlgebraTerm(algebra.OuterJoin(algebra.Function(getPatternVariables(PairPattern(w, VariablePattern(algebra.Variable(v)))), convert(p2)),
                                                               E match { case AlgebraTerm(t) => t },
                                                               algebra.Selection(algebra.Function(getPatternVariables(VariablePattern(algebra.Variable(v))), convert(p1)),
                                                                                 convert(x)))))
                }
                case _ => {
                  /** Rule C10 */
                  T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                 u, PairPattern(w, VariablePattern(algebra.Variable(v))),
                                 AlgebraTerm(algebra.OuterUnnest(algebra.Function(getPatternVariables(w), convert(x)),
                                                                 algebra.Function(getPatternVariables(PairPattern(w, VariablePattern(algebra.Variable(v)))), convert(canonical.MergeMonoid(BoolType, AndMonoid(), p1, p2))),
                                                                 E match { case AlgebraTerm(t) => t })))
                }
              }
            }
          }
        }
      }
    }
  }

  def unnest(e: canonical.TypedExpression) = {
    T(CalculusTerm(e), EmptyPattern, EmptyPattern, AlgebraTerm(algebra.Empty)) match { case AlgebraTerm(t) => t }
  }
  
}