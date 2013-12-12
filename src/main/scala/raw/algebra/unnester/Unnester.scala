package raw.algebra.unnester

import raw._
import raw.calculus._

object Unnester {

  private sealed abstract class Term
  private case class CalculusTerm(t: canonical.TypedExpression) extends Term
  private case class AlgebraTerm(t: algebra.Algebra) extends Term
  
  private sealed abstract class Pattern
  private case object EmptyPattern extends Pattern
  private case class VariablePattern(v: canonical.Variable) extends Pattern
  private case class PairPattern(a: Pattern, b: Pattern) extends Pattern
  
  /** Helper object to pattern match nested comprehensions. */
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

  /** This method returns the variable in the path. */
  def getPathVariable(p: canonical.Path): canonical.Variable = {
    import raw.calculus.canonical._
    
    p match {
      case VariablePath(v) => v
      case InnerPath(p, _) => getPathVariable(p)
    }
  }
  
  /** This method returns true if the comprehension 'c' does not depend on variables defined
   *  in 's' generators. */
  private def isIndependent(s: List[canonical.Generator], c: canonical.Comprehension): Boolean = {
    import raw.calculus.canonical._
    
    val gens = s.map{ case Generator(v, _) => v }
    
    def apply(e: TypedExpression): Boolean = e match {
      case n : Null => true
      case c : Constant => true
      case v : Variable => !gens.contains(v)
      case RecordProjection(_, e, _) => apply(e)
      case RecordConstruction(_, atts) => !atts.map(att => apply(att.e)).contains(false)
      case IfThenElse(_, e1, e2, e3) => apply(e1) && apply(e2) && apply(e3)
      case BinaryOperation(_, _, e1, e2) => apply(e1) && apply(e2)
      case z : EmptySet => true
      case z : EmptyBag => true
      case z : EmptyList => true
      case ConsCollectionMonoid(_, _, e) => apply(e)
      case MergeMonoid(_, _, e1, e2) => apply(e1) && apply(e2)
      case Comprehension(_, _, e, gs, pred) => 
        apply(e) && !gs.map{ case Generator(_, p) => !gens.contains(getPathVariable(p)) }.contains(false) && apply(pred)
      case Not(e) => apply(e)
    }
    
    apply(c)    
  }
  
  /** This method returns a new expression with nested comprehension 'c' replaced by variable 'v'. */
  private def applyVariable(e: canonical.TypedExpression, v: canonical.Variable, c: canonical.Comprehension): canonical.TypedExpression = {
    import raw.calculus.canonical._
    
    e match {
      case n : Null => n
      case c : Constant => c
      case v : Variable => v
      case RecordProjection(t, e, name) => RecordProjection(t, applyVariable(e, v, c), name)
      case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, applyVariable(att.e, v, c))))
      case IfThenElse(t, e1, e2, e3) => IfThenElse(t, applyVariable(e1, v, c), applyVariable(e2, v, c), applyVariable(e3, v, c))
      case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, applyVariable(e1, v, c), applyVariable(e2, v, c))
      case z : EmptySet => z
      case z : EmptyBag => z
      case z : EmptyList => z
      case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, applyVariable(e, v, c))
      case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, applyVariable(e1, v, c), applyVariable(e2, v, c))
      case c1 : Comprehension if c == c1 => v
      case Comprehension(t, m, e, gs, pred) => Comprehension(t, m, applyVariable(e, v, c), gs, applyVariable(pred, v, c))
      case Not(e) => Not(applyVariable(e, v, c))
    }
  }
 
  /** This method returns the set of canonical variables in the pattern. */
  private def getPatternVariables(p: Pattern): Set[canonical.Variable] = p match {
    case EmptyPattern => Set()
    case VariablePattern(v) => Set(v)
    case PairPattern(a, b) => getPatternVariables(a) ++ getPatternVariables(b)
  }
  
  /** This method returns the set of variables in the expression.
   *  The expression cannot contain nested comprehensions.
   */
  private def getExpressionVariables(e: canonical.TypedExpression): Set[canonical.Variable] = {
    import raw.calculus.canonical._
    
    e match {
      case n : Null => Set()
      case c : Constant => Set()
      case v : Variable => Set(v)
      case RecordProjection(_, e, _) => getExpressionVariables(e)
      case RecordConstruction(t, atts) => atts.flatMap(att => getExpressionVariables(att.e)).toSet
      case IfThenElse(_, e1, e2, e3) => getExpressionVariables(e1) ++ getExpressionVariables(e2) ++ getExpressionVariables(e3) 
      case BinaryOperation(_, _, e1, e2) => getExpressionVariables(e1) ++ getExpressionVariables(e2)
      case z : EmptySet => Set()
      case z : EmptyBag => Set()
      case z : EmptyList => Set()
      case ConsCollectionMonoid(_, _, e) => getExpressionVariables(e)
      case MergeMonoid(_, _, e1, e2) => getExpressionVariables(e1) ++ getExpressionVariables(e2)
      case c : Comprehension => throw RawInternalException("unexpected Comprehension found")
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
    val p1 = pred.filter(p => !hasNestedComprehension(p) && getExpressionVariables(p).subsetOf(getPatternVariables(right)))
    val p2 = pred.filter(p => !hasNestedComprehension(p) && getExpressionVariables(p).subsetOf(getPatternVariables(left)) && getExpressionVariables(p).subsetOf(getPatternVariables(right)))
    val p3 = pred.filter(p => !p1.contains(p) && !p2.contains(p))
    (fold(p1), fold(p2), fold(p3))
  }
   
  /** This method converts a canonical typed expression to the equivalent algebra expression.
   *  Algebra expressions only use primitive types and primitive monoids.
   *  The remaining is handled by the algebra operators.
   */
  private def convertExpression(e: canonical.TypedExpression): algebra.Expression = e match {
    case n : canonical.Null =>  throw RawInternalException("unexpected Null: expression not converted into algebra")
    case canonical.BoolConst(v) => algebra.BoolConst(v)
    case canonical.IntConst(v) => algebra.IntConst(v)
    case canonical.FloatConst(v) => algebra.FloatConst(v)
    case canonical.StringConst(v) => algebra.StringConst(v)
    case v : canonical.Variable => algebra.Variable(v)
    case canonical.RecordProjection(t : PrimitiveType, e, name) => algebra.RecordProjection(t, convertExpression(e), name)
    case r : canonical.RecordProjection => throw RawInternalException("unexpected non-primitive type: expression not converted into algebra")
    case canonical.RecordConstruction(t : RecordType, atts) => algebra.RecordConstruction(t, atts.map(att => algebra.AttributeConstruction(att.name, convertExpression(att.e))))
    case r : canonical.RecordConstruction => throw RawInternalException("unexpected non-primitive type: expression not converted into algebra")
    case canonical.IfThenElse(t : PrimitiveType, e1, e2, e3) => algebra.IfThenElse(t, convertExpression(e1), convertExpression(e2), convertExpression(e3))
    case i : canonical.IfThenElse => throw RawInternalException("unexpected non-primitive type: expression not converted into algebra")
    case canonical.BinaryOperation(t : PrimitiveType, op, e1, e2) => algebra.BinaryOperation(t, op, convertExpression(e1), convertExpression(e2))
    case b : canonical.BinaryOperation => throw RawInternalException("unexpected non-primitive type: expression not converted into algebra")
    case z : canonical.EmptySet =>  throw RawInternalException("unexpected EmptySet: expression not converted into algebra")
    case z : canonical.EmptyBag =>  throw RawInternalException("unexpected EmptyBag: expression not converted into algebra")
    case z : canonical.EmptyList =>  throw RawInternalException("unexpected EmptyList: expression not converted into algebra")
    case canonical.ConsCollectionMonoid(t, m, e) => throw RawInternalException("unexpected ConsCollectionMonoid: expression not converted into algebra")
    case canonical.MergeMonoid(t : PrimitiveType, m : PrimitiveMonoid, e1, e2) => algebra.MergeMonoid(t, m, convertExpression(e1), convertExpression(e2))
    case canonical.MergeMonoid(t : PrimitiveType, _, _, _) => throw RawInternalException("unexpected non-primitive monoid: expression not converted into algebra")
    case canonical.MergeMonoid(_, m : PrimitiveMonoid, e1, e2) => throw RawInternalException("unexpected non-primitive type: expression not converted into algebra")
    case c : canonical.Comprehension => throw RawInternalException("unexpected Comprehension: expression not converted into algebra")
    case canonical.Not(e) => algebra.Not(convertExpression(e))
  } 

  /** This method converts a set of canonical variables to algebra variables. */
  private def convertVariables(vs: Set[canonical.Variable]) =  vs.map(v => algebra.Variable(v))
  
  /** Implementation of the Query Unnesting Algorithm described in Figure 11, page 37 of [1].
    */
  private def T(e: Term, u: Pattern, w: Pattern, E: Term): Term = e match {
    case CalculusTerm(canonical.Comprehension(t, m, e1, s, p)) => p match {
      case NestedComprehension(ep) if isIndependent(s, ep) => {
        /** Rule C11 */
        val v = canonical.Variable(normalizer.Variable(parser.Variable(ep.monoidType)))
        T(CalculusTerm(canonical.Comprehension(t, m, e1, s, applyVariable(p, v, ep))),
                       u, PairPattern(w, VariablePattern(v)),
                       T(CalculusTerm(ep), w, w, E))
      }
      case _ => e match {
        case CalculusTerm(canonical.Comprehension(t, m, e1, List(), p)) => e1 match {
           case NestedComprehension(ep) => {
             /** Rule C12 */
             val v = canonical.Variable(normalizer.Variable(parser.Variable(ep.monoidType)))
             T(CalculusTerm(canonical.Comprehension(t, m, applyVariable(e1, v, ep), List(), p)),
                            u, PairPattern(w, VariablePattern(v)),
                            T(CalculusTerm(ep), w, w, E))
           }
           case _ => {
             if (u == EmptyPattern) {
               /** Rule C5 */
               AlgebraTerm(algebra.Reduce(m,
                                          algebra.Function(convertVariables(getPatternVariables(w)), convertExpression(e1)),
                                          algebra.Function(convertVariables(getPatternVariables(w)), convertExpression(p)),
                                          E match { case AlgebraTerm(t) => t }))
             } else {
               /** Rule C8 */
               AlgebraTerm(algebra.Nest(m,
                                        algebra.Function(convertVariables(getPatternVariables(w)), convertExpression(e1)),
                                        algebra.FunctionVars(convertVariables(getPatternVariables(w)), convertVariables(getPatternVariables(u))),
                                        algebra.Function(convertVariables(getPatternVariables(w)), convertExpression(p)),
                                        algebra.FunctionVars(convertVariables(getPatternVariables(w)), convertVariables(getPatternVariables(w) -- getPatternVariables(u))),
                                        E match { case AlgebraTerm(t) => t }))
             }
           }
        }
        case _ => e match {
          case CalculusTerm(canonical.Comprehension(t, m, e1, canonical.Generator(v, x) :: r, p)) => {
            val (p1, p2, p3) = splitPredicate(p, w, VariablePattern(v))
            if (u == EmptyPattern) {
              if (w == EmptyPattern) {
                /** Rule C4 */
                T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                               u, VariablePattern(v),
                               AlgebraTerm(algebra.Select(algebra.Function(convertVariables(getPatternVariables(VariablePattern(v))), convertExpression(canonical.MergeMonoid(BoolType, AndMonoid(), p1, p2))),
                                                          algebra.Scan(algebra.FunctionPath(convertVariables(getPatternVariables(VariablePattern(v))), x)))))
              } else {
                x match {
                  case x : canonical.VariablePath => {
                    /** Rule C6 */
                    T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                   u, PairPattern(w, VariablePattern(v)),
                                   AlgebraTerm(algebra.Join(algebra.Function(convertVariables(getPatternVariables(PairPattern(w, VariablePattern(v)))), convertExpression(p2)),
                                                            E match { case AlgebraTerm(t) => t },
                                                            algebra.Select(algebra.Function(convertVariables(getPatternVariables(VariablePattern(v))), convertExpression(p1)),
                                                                           algebra.Scan(algebra.FunctionPath(convertVariables(getPatternVariables(VariablePattern(v))), x))))))
                  }
                  case x : canonical.InnerPath => {
                    /** Rule C7 */
                    T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                   u, PairPattern(w, VariablePattern(v)),
                                   AlgebraTerm(algebra.Unnest(algebra.FunctionPath(convertVariables(getPatternVariables(w)), x),
                                                              algebra.Function(convertVariables(getPatternVariables(PairPattern(w, VariablePattern(v)))), convertExpression(canonical.MergeMonoid(BoolType, AndMonoid(), p1, p2))),
                                                              E match { case AlgebraTerm(t) => t })))
                  }
                }
              }
            } else {
              x match {
                case x : canonical.VariablePath => {
                  /** Rule C9 */
                  T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                 u, PairPattern(w, VariablePattern(v)),
                                 AlgebraTerm(algebra.OuterJoin(algebra.Function(convertVariables(getPatternVariables(PairPattern(w, VariablePattern(v)))), convertExpression(p2)),
                                                               E match { case AlgebraTerm(t) => t },
                                                               algebra.Select(algebra.Function(convertVariables(getPatternVariables(VariablePattern(v))), convertExpression(p1)),
                                                                              algebra.Scan(algebra.FunctionPath(convertVariables(getPatternVariables(VariablePattern(v))), x))))))
                }
                case x : canonical.InnerPath => {
                  /** Rule C10 */
                  T(CalculusTerm(canonical.Comprehension(t, m, e1, r, p3)),
                                 u, PairPattern(w, VariablePattern(v)),
                                 AlgebraTerm(algebra.OuterUnnest(algebra.FunctionPath(convertVariables(getPatternVariables(w)), x),
                                                                 algebra.Function(convertVariables(getPatternVariables(PairPattern(w, VariablePattern(v)))), convertExpression(canonical.MergeMonoid(BoolType, AndMonoid(), p1, p2))),
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