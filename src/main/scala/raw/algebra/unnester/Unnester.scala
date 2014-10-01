package raw.algebra.unnester

import raw._
import raw.catalog._

object Unnester {

  private sealed abstract class Term
  private case class CalculusTerm(t: calculus.canonical.TypedExpression) extends Term
  private case class AlgebraTerm(t: algebra.Algebra) extends Term

  private sealed abstract class Pattern
  private case object EmptyPattern extends Pattern
  private case class VariablePattern(v: calculus.canonical.Variable) extends Pattern
  private case class PairPattern(a: Pattern, b: Pattern) extends Pattern

  /** Helper object to pattern match nested comprehensions. */
  private object NestedComprehension {
    import raw.calculus.canonical._

    def unapply(e: Expression): Option[Comprehension] = e match {
      case RecordProjection(_, NestedComprehension(e), _) => Some(e)
      case RecordConstruction(t, atts) => atts.map(att => att.e match {
        case NestedComprehension(e) => Some(e)
        case _                      => None
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
      case c: Comprehension => Some(c)
      case _ => None
    }
  }

  /** Check whether the expression has a nested comprehension. */
  private def hasNestedComprehension(e: calculus.canonical.TypedExpression): Boolean = e match {
    case NestedComprehension(_) => true
    case _                      => false
  }

  /** Return variable in the path. */
  def getPathVariable(p: calculus.canonical.Path): calculus.canonical.Variable = {
    import raw.calculus.canonical._

    p match {
      case VariablePath(v) => v
      case InnerPath(p, _) => getPathVariable(p)
    }
  }

  /** Check whether the comprehension 'c' does not depend on variables in the list of generators 's'.
   */
  private def isIndependent(s: List[calculus.canonical.Generator], c: calculus.canonical.Comprehension): Boolean = {
    import raw.calculus.canonical._

    val gens = s.map { case Generator(v, _) => v }

    def apply(e: TypedExpression): Boolean = e match {
      case Null                          => true
      case c: Constant                   => true
      case v: Variable                   => !gens.contains(v)
      case RecordProjection(_, e, _)     => apply(e)
      case RecordConstruction(_, atts)   => !atts.map(att => apply(att.e)).contains(false)
      case IfThenElse(_, e1, e2, e3)     => apply(e1) && apply(e2) && apply(e3)
      case BinaryOperation(_, _, e1, e2) => apply(e1) && apply(e2)
      case EmptySet                      => true
      case EmptyBag                      => true
      case EmptyList                     => true
      case ConsCollectionMonoid(_, _, e) => apply(e)
      case MergeMonoid(_, _, e1, e2)     => apply(e1) && apply(e2)
      case Comprehension(_, _, e, gs, pred) =>
        apply(e) && !gs.map { case Generator(_, p) => !gens.contains(getPathVariable(p)) }.contains(false) && apply(pred)
      case Not(e)           => apply(e)
      case FloatToInt(e)    => apply(e)
      case FloatToString(e) => apply(e)
      case IntToFloat(e)    => apply(e)
      case IntToString(e)   => apply(e)
      case StringToBool(e)  => apply(e)
      case StringToInt(e)   => apply(e)
      case StringToFloat(e) => apply(e)

    }

    apply(c)
  }

  /** Return a new expression with nested comprehension 'c' replaced by variable 'v'. */
  private def applyVariable(e: calculus.canonical.TypedExpression, v: calculus.canonical.Variable, c: calculus.canonical.Comprehension): calculus.canonical.TypedExpression = {
    import raw.calculus.canonical._

    e match {
      case Null                             => Null
      case c: Constant                      => c
      case v: Variable                      => v
      case RecordProjection(t, e, name)     => RecordProjection(t, applyVariable(e, v, c), name)
      case RecordConstruction(t, atts)      => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, applyVariable(att.e, v, c))))
      case IfThenElse(t, e1, e2, e3)        => IfThenElse(t, applyVariable(e1, v, c), applyVariable(e2, v, c), applyVariable(e3, v, c))
      case BinaryOperation(t, op, e1, e2)   => BinaryOperation(t, op, applyVariable(e1, v, c), applyVariable(e2, v, c))
      case EmptySet                         => EmptySet
      case EmptyBag                         => EmptyBag
      case EmptyList                        => EmptyList
      case ConsCollectionMonoid(t, m, e)    => ConsCollectionMonoid(t, m, applyVariable(e, v, c))
      case MergeMonoid(t, m, e1, e2)        => MergeMonoid(t, m, applyVariable(e1, v, c), applyVariable(e2, v, c))
      case c1: Comprehension if c == c1     => v
      case Comprehension(t, m, e, gs, pred) => Comprehension(t, m, applyVariable(e, v, c), gs, applyVariable(pred, v, c))
      case Not(e)                           => Not(applyVariable(e, v, c))
    }
  }

  /** Return the set of canonical variables in the pattern. */
  private def getPatternVariables(p: Pattern): List[calculus.canonical.Variable] = p match {
    case EmptyPattern       => List()
    case VariablePattern(v) => List(v)
    case PairPattern(a, b)  => getPatternVariables(a) ++ getPatternVariables(b)
  }

  /** Return the set of variables in the expression.
   *  The expression cannot contain nested comprehensions.
   */
  private def getExpressionVariables(e: calculus.canonical.TypedExpression): Set[calculus.canonical.Variable] = {
    import raw.calculus.canonical._

    e match {
      case Null                          => Set()
      case c: Constant                   => Set()
      case v: Variable                   => Set(v)
      case RecordProjection(_, e, _)     => getExpressionVariables(e)
      case RecordConstruction(t, atts)   => atts.flatMap(att => getExpressionVariables(att.e)).toSet
      case IfThenElse(_, e1, e2, e3)     => getExpressionVariables(e1) ++ getExpressionVariables(e2) ++ getExpressionVariables(e3)
      case BinaryOperation(_, _, e1, e2) => getExpressionVariables(e1) ++ getExpressionVariables(e2)
      case EmptySet                      => Set()
      case EmptyBag                      => Set()
      case EmptyList                     => Set()
      case ConsCollectionMonoid(_, _, e) => getExpressionVariables(e)
      case MergeMonoid(_, _, e1, e2)     => getExpressionVariables(e1) ++ getExpressionVariables(e2)
      case c: Comprehension              => throw RawInternalException("unexpected Comprehension")
      case Not(e)                        => getExpressionVariables(e)
      case FloatToInt(e)                 => getExpressionVariables(e)
      case FloatToString(e)              => getExpressionVariables(e)
      case IntToFloat(e)                 => getExpressionVariables(e)
      case IntToString(e)                => getExpressionVariables(e)
      case StringToBool(e)               => getExpressionVariables(e)
      case StringToInt(e)                => getExpressionVariables(e)
      case StringToFloat(e)              => getExpressionVariables(e)
    }
  }

  /** Implementation of the 'split_predicate(p, left, right)' method described in page 36 of [1].
   */
  private def splitPredicate(p: calculus.canonical.TypedExpression, left: Pattern, right: Pattern) = {

    /** Flatten a predicate in CNF form into a list of predicates. */
    def flatten(e: calculus.canonical.TypedExpression): List[calculus.canonical.TypedExpression] = e match {
      case calculus.canonical.MergeMonoid(t, calculus.AndMonoid(), e1, e2) => flatten(e1) ::: flatten(e2)
      case _ => List(e)
    }

    /** Fold a list of CNF predicates into a single predicate. */
    def fold(e: List[calculus.canonical.TypedExpression]) = {
      if (e.isEmpty) {
        calculus.canonical.BoolConst(true)
      } else {
        val head = e.head
        val rest = e.drop(1)
        rest.foldLeft(head)((a, b) => calculus.canonical.MergeMonoid(BoolType, calculus.AndMonoid(), a, b))
      }
    }

    val pred = flatten(p)

    val p1 = pred.filter(p =>
      !hasNestedComprehension(p) &&
        getExpressionVariables(p).subsetOf(getPatternVariables(right).toSet)
    )
    val p2 = pred.filter(p =>
      !hasNestedComprehension(p) &&
        !getExpressionVariables(p).intersect(getPatternVariables(left).toSet).isEmpty &&
        !getExpressionVariables(p).intersect(getPatternVariables(right).toSet).isEmpty &&
        getExpressionVariables(p).subsetOf(getPatternVariables(left).toSet.union(getPatternVariables(right).toSet))
    )
    val p3 = pred.filter(p => !p1.contains(p) && !p2.contains(p))

    (fold(p1), fold(p2), fold(p3))
  }

  private def convertVariable(v: calculus.canonical.Variable, p: Pattern) = {
    val idx = getPatternVariables(p).indexOf(v)
    algebra.Argument(v.monoidType, idx)
  }

  /** Convert a canonical typed expression to the equivalent algebra expression.
   *
   *  Algebra expressions only use primitive monoids.
   *  Collection monoids are handled the by algebraic operators.
   *
   */
  private def convertExpression(e: calculus.canonical.TypedExpression, p: Pattern): algebra.Expression = {
    e match {
      case calculus.canonical.BoolConst(v)                   => algebra.BoolConst(v)
      case calculus.canonical.IntConst(v)                    => algebra.IntConst(v)
      case calculus.canonical.FloatConst(v)                  => algebra.FloatConst(v)
      case calculus.canonical.StringConst(v)                 => algebra.StringConst(v)
      case v: calculus.canonical.Variable                    => convertVariable(v, p)
      case calculus.canonical.RecordProjection(t, e, name)   => algebra.RecordProjection(t, convertExpression(e, p), name)
      case calculus.canonical.RecordConstruction(t, atts)    => algebra.RecordConstruction(t, atts.map(att => algebra.AttributeConstruction(att.name, convertExpression(att.e, p))))
      case calculus.canonical.IfThenElse(t, e1, e2, e3)      => algebra.IfThenElse(t, convertExpression(e1, p), convertExpression(e2, p), convertExpression(e3, p))
      case calculus.canonical.BinaryOperation(t, op, e1, e2) => algebra.BinaryOperation(t, op, convertExpression(e1, p), convertExpression(e2, p))
      case calculus.canonical.MergeMonoid(t, m: calculus.PrimitiveMonoid, e1, e2) =>
        algebra.MergeMonoid(t, convertPrimitiveMonoid(m), convertExpression(e1, p), convertExpression(e2, p))
      case calculus.canonical.Not(e)           => algebra.Not(convertExpression(e, p))
      case calculus.canonical.FloatToInt(e)    => algebra.FloatToInt(convertExpression(e, p))
      case calculus.canonical.FloatToString(e) => algebra.FloatToString(convertExpression(e, p))
      case calculus.canonical.IntToFloat(e)    => algebra.IntToFloat(convertExpression(e, p))
      case calculus.canonical.IntToString(e)   => algebra.IntToString(convertExpression(e, p))
      case calculus.canonical.StringToBool(e)  => algebra.StringToBool(convertExpression(e, p))
      case calculus.canonical.StringToInt(e)   => algebra.StringToInt(convertExpression(e, p))
      case calculus.canonical.StringToFloat(e) => algebra.StringToFloat(convertExpression(e, p))
      case _                                   => throw RawInternalException("unexpected collection calculus term")
    }
  }

  private def convertPath(p: calculus.canonical.Path, w: Pattern): algebra.Path = p match {
    case calculus.canonical.VariablePath(v)    => algebra.ArgumentPath(getPatternVariables(w).indexOf(v))
    case calculus.canonical.InnerPath(p, name) => algebra.InnerPath(convertPath(p, w), name)
  }

  private def convertPrimitiveMonoid(m: calculus.PrimitiveMonoid): algebra.PrimitiveMonoid = m match {
    case m: calculus.SumMonoid      => algebra.SumMonoid
    case m: calculus.MultiplyMonoid => algebra.MultiplyMonoid
    case m: calculus.MaxMonoid      => algebra.MaxMonoid
    case m: calculus.OrMonoid       => algebra.OrMonoid
    case m: calculus.AndMonoid      => algebra.AndMonoid
  }

  private def convertCollectionMonoid(m: calculus.CollectionMonoid): algebra.CollectionMonoid = m match {
    case m: calculus.SetMonoid  => algebra.SetMonoid
    case m: calculus.BagMonoid  => algebra.BagMonoid
    case m: calculus.ListMonoid => algebra.ListMonoid
  }

  /** Convert a monoid AST node from the calculus representation to the equivalent algebra representation. */
  private def convertMonoid(m: calculus.Monoid): algebra.Monoid = m match {
    case m: calculus.PrimitiveMonoid  => convertPrimitiveMonoid(m)
    case m: calculus.CollectionMonoid => convertCollectionMonoid(m)
  }

  private def buildArgumentList(w: Pattern, u: Pattern) =
    getPatternVariables(u).map(u => convertVariable(u, w))

  private def reducePattern(l: Pattern, r: Pattern): Pattern = {
    val rs = getPatternVariables(r).toSet
    def recurse(p: Pattern): Pattern = p match {
      case EmptyPattern                         => EmptyPattern
      case VariablePattern(v) if rs.contains(v) => EmptyPattern
      case VariablePattern(v)                   => VariablePattern(v)
      case PairPattern(a, b)                    => PairPattern(recurse(a), recurse(b))
    }
    recurse(l)
  }

  /** Implementation of the Query Unnesting Algorithm described in Figure 11, page 37 of [1].
   *
   *  Typos found & fixed in the paper algorithm (e.g. use of variable 'np' in rule C4)
   */
  private def T(e: Term, u: Pattern, w: Pattern, E: Term): AlgebraTerm = e match {
    case CalculusTerm(calculus.canonical.Comprehension(t, m, e1, s, p)) => p match {
      case NestedComprehension(ep) if isIndependent(s, ep) => {
        /** Rule C11 */
        val v = calculus.canonical.Variable(calculus.normalizer.Variable(ep.monoidType, ep.hashCode().toString()))
        T(
          CalculusTerm(calculus.canonical.Comprehension(t, m, e1, s, applyVariable(p, v, ep))),
          u,
          PairPattern(w, VariablePattern(v)),
          T(CalculusTerm(ep), w, w, E))
      }
      case _ => e match {
        case CalculusTerm(calculus.canonical.Comprehension(t, m, e1, List(), p)) => e1 match {
          case NestedComprehension(ep) => {
            /** Rule C12 */
            val v = calculus.canonical.Variable(calculus.normalizer.Variable(ep.monoidType, ep.hashCode().toString()))
            T(
              CalculusTerm(calculus.canonical.Comprehension(t, m, applyVariable(e1, v, ep), List(), p)),
              u,
              PairPattern(w, VariablePattern(v)),
              T(CalculusTerm(ep), w, w, E))
          }
          case _ => {
            if (u == EmptyPattern) {
              /** Rule C5 */
              AlgebraTerm(
                algebra.Reduce(convertMonoid(m),
                  convertExpression(e1, w),
                  convertExpression(p, w),
                  E match {
                    case AlgebraTerm(t) => t
                    case _              => throw RawInternalException("unexpected calculus term")
                  }))
            } else {
              /** Rule C8 */
              AlgebraTerm(
                algebra.Nest(convertMonoid(m),
                  convertExpression(e1, w),
                  buildArgumentList(w, u),
                  convertExpression(p, w),
                  buildArgumentList(w, reducePattern(w, u)),
                  E match {
                    case AlgebraTerm(t) => t
                    case _              => throw RawInternalException("unexpected calculus term")
                  }))
            }
          }
        }
        case _ => e match {
          case CalculusTerm(calculus.canonical.Comprehension(t, m, e1, calculus.canonical.Generator(v, x) :: r, p)) => {
            val (p1, p2, p3) = splitPredicate(p, w, VariablePattern(v))
            if (u == EmptyPattern) {
              if (w == EmptyPattern) {
                /** Rule C4 */
                T(
                  CalculusTerm(calculus.canonical.Comprehension(t, m, e1, r, p3)),
                  u,
                  VariablePattern(v),
                  AlgebraTerm(
                    algebra.Select(
                      convertExpression(calculus.canonical.MergeMonoid(BoolType, calculus.AndMonoid(), p1, p2), VariablePattern(v)),
                      algebra.Scan(
                        getPathVariable(x).v.name))))
              } else {
                x match {
                  case x: calculus.canonical.VariablePath => {
                    /** Rule C6 */
                    T(
                      CalculusTerm(calculus.canonical.Comprehension(t, m, e1, r, p3)),
                      u,
                      PairPattern(w, VariablePattern(v)),
                      AlgebraTerm(
                        algebra.Join(
                          convertExpression(p2, PairPattern(w, VariablePattern(v))),
                          E match {
                            case AlgebraTerm(t) => t
                            case _              => throw RawInternalException("unexpected calculus term")
                          },
                          algebra.Select(
                            convertExpression(p1, VariablePattern(v)),
                            algebra.Scan(
                              getPathVariable(x).v.name)))))
                  }
                  case x: calculus.canonical.InnerPath => {
                    /** Rule C7 */
                    T(
                      CalculusTerm(calculus.canonical.Comprehension(t, m, e1, r, p3)),
                      u,
                      PairPattern(w, VariablePattern(v)),
                      AlgebraTerm(
                        algebra.Unnest(
                          convertPath(x, w),
                          convertExpression(calculus.canonical.MergeMonoid(BoolType, calculus.AndMonoid(), p1, p2), PairPattern(w, VariablePattern(v))),
                          E match {
                            case AlgebraTerm(t) => t
                            case _              => throw RawInternalException("unexpected calculus term")
                          })))
                  }
                }
              }
            } else {
              x match {
                case x: calculus.canonical.VariablePath => {
                  /** Rule C9 */
                  T(
                    CalculusTerm(calculus.canonical.Comprehension(t, m, e1, r, p3)),
                    u,
                    PairPattern(w, VariablePattern(v)),
                    AlgebraTerm(
                      algebra.OuterJoin(convertExpression(p2, PairPattern(w, VariablePattern(v))),
                        E match {
                          case AlgebraTerm(t) => t
                          case _              => throw RawInternalException("unexpected calculus term")
                        },
                        algebra.Select(
                          convertExpression(p1, VariablePattern(v)),
                          algebra.Scan(
                            getPathVariable(x).v.name)))))
                }
                case x: calculus.canonical.InnerPath => {
                  /** Rule C10 */
                  T(
                    CalculusTerm(calculus.canonical.Comprehension(t, m, e1, r, p3)),
                    u,
                    PairPattern(w, VariablePattern(v)),
                    AlgebraTerm(
                      algebra.OuterUnnest(
                        convertPath(x, w),
                        convertExpression(calculus.canonical.MergeMonoid(BoolType, calculus.AndMonoid(), p1, p2), PairPattern(w, VariablePattern(v))),
                        E match {
                          case AlgebraTerm(t) => t
                          case _              => throw RawInternalException("unexpected calculus term")
                        })))
                }
              }
            }
          }
          case CalculusTerm(_) => throw RawInternalException("unexpected calculus term")
          case _: AlgebraTerm  => throw RawInternalException("unexpected algebra term")
        }
      }
    }
    /** The following rule is not present in the original algorithm.
     *  
     *  It handles transformations such as:
     *  	for x <- (A union B) yield set x
     *  where the result is:
     *    (for (x1 <- A) yield set x1) merge set (for (x2 <- B) yield set x2)
     */
    case CalculusTerm(k @calculus.canonical.MergeMonoid(t, m, e1, e2)) => {
      AlgebraTerm(
        algebra.Merge(
          convertMonoid(m),
          T(CalculusTerm(e1), u, w, e).t,
          T(CalculusTerm(e2), u, w, e).t))
    }
    case CalculusTerm(_) => throw RawInternalException("unexpected calculus term")
    case _: AlgebraTerm  => throw RawInternalException("unexpected algebra term")
  }

  def apply(e: calculus.canonical.TypedExpression) = {
    T(CalculusTerm(e), EmptyPattern, EmptyPattern, AlgebraTerm(algebra.Empty)) match { case AlgebraTerm(t) => t }
  }

}
