package raw.calculus.canonical

import raw._
import raw.calculus._
import raw.calculus.normalizer._

object Canonical {

  /** Convert the calculus expression into its canonical form.
   *  According to [1], the canonical form is defined as:
   *  "That is, all generator domains have been reduced to paths
   *   and all predicates have been collected to the right of the comprehension
   *   into pred by anding them together (pred set to true if no predicate exists).
   */
  def canonical(e: TypedExpression): TypedExpression = {
    
    def apply(e: TypedExpression): TypedExpression = e match {
      case Null() => e
      case BoolConst(_) => e
      case IntConst(_) => e
      case FloatConst(_) => e
      case StringConst(_) => e
      case Variable(_) => e
      case ClassExtent(_, _) => e
      case RecordProjection(t, e, name) => RecordProjection(t, apply(e), name)
      case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, apply(att.e))))
      case IfThenElse(t, e1, e2, e3) => IfThenElse(t, apply(e1), apply(e2), apply(e3))
      case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, apply(e1), apply(e2))
      case FunctionAbstraction(t, v, e) => FunctionAbstraction(t, v, apply(e)) 
      case FunctionApplication(t, e1, e2) => FunctionApplication(t, apply(e1), apply(e2))
      case EmptySet() => e
      case EmptyBag() => e
      case EmptyList() => e
      case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, apply(e))
      case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, apply(e1), apply(e2))
      case Not(e) => Not(apply(e))
      case Comprehension(t, m, e, qs) => {
        val gs = qs.collect{ case Generator(v, e) => Generator(v, apply(e)) }
        val ps = qs.collect{ case e: TypedExpression => apply(e) }
        val pred = ps.foldLeft(BoolConst(true).asInstanceOf[TypedExpression])((a, b) => MergeMonoid(BoolType, AndMonoid(), a, b))
        Comprehension(t, m, apply(e), gs ::: List(pred))
      }
    }
    val n = Normalizer.normalize(e)
    apply(n)
  }
}