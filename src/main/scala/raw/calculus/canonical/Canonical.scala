package raw.calculus.canonical

import raw._
import raw.calculus._

object Canonical {
  
  /** This method flattens a predicate in CNF form into a list of predicates. */
  private def flatten(e: TypedExpression): List[TypedExpression] = e match {
     case MergeMonoid(t, AndMonoid(), e1, e2) => flatten(e1) ::: flatten(e2)
     case _ => List(e)
   }
  
  /** This method converts the expression to CNF.
   *  A classical algorithm is applied.
   *  'IfThenElse()' conditions, where the 'then' and 'else' parts are booleans, are also converted to CNF.
   *  
   *  FIXME:
   *  [MSB] The case MergeMonoid(t, OrMonoid(), e1, e2) can be very inefficient if e1 and e2 are
   *        dijunctions of large numbers of literals. There are more efficient methods but require
   *        introducing new variables.
   */
  private def convertToCNF(e: TypedExpression): TypedExpression = e match {
    case n : Null => n
    case c : Constant => c
    case v : Variable => v
    case RecordProjection(t, e, name) => RecordProjection(t, convertToCNF(e), name)
    case RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, convertToCNF(att.e))))
    case IfThenElse(t, e1, e2, e3) if e2.monoidType == BoolType => println("here");
      convertToCNF(MergeMonoid(BoolType, OrMonoid(), MergeMonoid(BoolType, AndMonoid(), e1, e2), MergeMonoid(BoolType, AndMonoid(), Not(e1), e3)))
    case IfThenElse(t, e1, e2, e3) => println("e2 " + e2.monoidType); IfThenElse(t, convertToCNF(e1), convertToCNF(e2), convertToCNF(e3))
    case BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, convertToCNF(e1), convertToCNF(e2))
    case z : EmptySet => z
    case z : EmptyBag => z
    case z : EmptyList => z
    case ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, convertToCNF(e))
    case MergeMonoid(_, OrMonoid(), BoolConst(true), _) => BoolConst(true)
    case MergeMonoid(_, OrMonoid(), _, BoolConst(true)) => BoolConst(true)
    case MergeMonoid(t, OrMonoid(), BoolConst(false), e2) => convertToCNF(e2) 
    case MergeMonoid(t, OrMonoid(), e1, BoolConst(false)) => convertToCNF(e1)
    case MergeMonoid(t, OrMonoid(), e1, e2) => {
      val p = convertToCNF(e1)
      val q = convertToCNF(e2)
      val ps = flatten(p)
      val qs = flatten(q)
      val prod = for (p <- ps; q <- qs) yield MergeMonoid(BoolType, OrMonoid(), p, q)
      val head = prod.head
      val rest = prod.drop(1)
      convertToCNF(rest.foldLeft(head)((a, b) => MergeMonoid(BoolType, AndMonoid(), a, b)))
    }
    case MergeMonoid(_, AndMonoid(), BoolConst(false), _) => BoolConst(false)
    case MergeMonoid(_, AndMonoid(), _, BoolConst(false)) => BoolConst(false)
    case MergeMonoid(t, AndMonoid(), BoolConst(true), e2) => convertToCNF(e2) 
    case MergeMonoid(t, AndMonoid(), e1, BoolConst(true)) => convertToCNF(e1)
    case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, convertToCNF(e1), convertToCNF(e2))
    case Not(Not(e)) => convertToCNF(e)
    case Not(MergeMonoid(t, AndMonoid(), e1, e2)) => convertToCNF(MergeMonoid(t, OrMonoid(), Not(e1), Not(e2)))
    case Not(MergeMonoid(t, OrMonoid(), e1, e2)) => convertToCNF(MergeMonoid(t, AndMonoid(), Not(e1), Not(e2)))    
    case Comprehension(t, m, e, gs, pred) => Comprehension(t, m, convertToCNF(e), gs, convertToCNF(pred))
    case Not(e) => Not(convertToCNF(e))
  }
  
  /** This method converts an expression used in the rhs of a generator into a Path.
   */
  def convertToPath(e: Expression): Path = e match {
    case v: Variable => VariablePath(v)
    case RecordProjection(t, e, name) => InnerPath(convertToPath(e), name)
    case _ => throw RawInternalException("unexpected expression in rhs of Generator")
  }
  
  /** This method converts the calculus expression into its canonical form.
   *  The expression must be normalized. This is enforced by taking as input an instance of the
   *  Normalizer calculus. The canonical form expression is returned Canonical calculus.
   *  
   *  According to [1], the canonical form is defined as:
   *  "That is, all generator domains have been reduced to paths
   *   and all predicates have been collected to the right of the comprehension
   *   into pred by anding them together (pred set to true if no predicate exists).
   */
  def canonical(e: normalizer.TypedExpression): TypedExpression = e match {
    case normalizer.Null() => Null()
    case normalizer.BoolConst(v) => BoolConst(v)
    case normalizer.StringConst(v) => StringConst(v)
    case normalizer.FloatConst(v) => FloatConst(v)
    case normalizer.IntConst(v) => IntConst(v)
    case v : normalizer.Variable => Variable(v)
    case normalizer.RecordProjection(t, e, name) => RecordProjection(t, canonical(e), name)
    case normalizer.RecordConstruction(t, atts) => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, canonical(att.e))))
    case normalizer.IfThenElse(t, e1, e2, e3) => IfThenElse(t, canonical(e1), canonical(e2), canonical(e3))
    case normalizer.BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, canonical(e1), canonical(e2))
    case normalizer.EmptySet() => EmptySet()
    case normalizer.EmptyBag() => EmptyBag()
    case normalizer.EmptyList() => EmptyList()
    case normalizer.ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(t, m, canonical(e))
    case normalizer.MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, canonical(e1), canonical(e2))
    case normalizer.Comprehension(t, m, e, qs) => {
      val gs = qs.collect{ case normalizer.Generator(v : normalizer.Variable, e) => Generator(Variable(v), convertToPath(canonical(e))) }
      val ps = qs.collect{ case e: normalizer.TypedExpression => canonical(e) }
      if (ps.isEmpty)
        Comprehension(t, m, canonical(e), gs, BoolConst(true))
      else {
        /* Merge predicates into one expression to convert it to CNF and then split the results again. */
        val head = ps.head
        val rest = ps.drop(1)
        val pred = rest.foldLeft(head)((a, b) => MergeMonoid(BoolType, AndMonoid(), a, b))
        Comprehension(t, m, canonical(e), gs, convertToCNF(pred))
      }
    }
    case normalizer.Not(e) => Not(canonical(e))
  }
}