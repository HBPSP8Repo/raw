package raw.calculus.canonical

import raw._
import raw.calculus._

object Canonical {

  /** Convert a (Normalizer) expression into a (Canonical) expression.
   *
   *  According to [1], the canonical form is defined as:
   *  "[...] all generator domains have been reduced to paths and all
   *   predicates have been collected to the right of the comprehension
   *   into pred by anding them together (pred set to true if no predicate
   *   exists).
   */
  def apply(e: normalizer.TypedExpression): TypedExpression = {
    /** Convert a sequence of record projections plus a variable into a Path.
     */
    def convertToPath(e: Expression): Path = e match {
      case v: Variable                  => VariablePath(v)
      case RecordProjection(t, e, name) => InnerPath(convertToPath(e), name)
      case _                            => throw RawInternalException("unexpected expression in rhs of Generator")
    }

    /** Convert expression into CNF form.
     *  A classical but rather inefficient algorithm is used.
     *  'IfThenElse()' conditions, where the 'then' and 'else' parts are booleans, are also converted to CNF.
     *
     *  FIXME:
     *  [MSB] The case MergeMonoid(t, OrMonoid(), e1, e2) can be very inefficient if e1 and e2 are
     *        dijunctions of large numbers of literals. There are more efficient methods but require
     *        introducing new variables.
     */
    def convertToCNF(e: TypedExpression): TypedExpression = {
      /** Flatten a predicate in CNF form into a list of predicates.
       */
      def flatten(e: TypedExpression): List[TypedExpression] = e match {
        case MergeMonoid(t, AndMonoid(), e1, e2) => flatten(e1) ::: flatten(e2)
        case _                                   => List(e)
      }

      e match {
        case n: Null                      => n
        case c: Constant                  => c
        case v: Variable                  => v
        case RecordProjection(t, e, name) => RecordProjection(t, convertToCNF(e), name)
        case RecordConstruction(t, atts)  => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, convertToCNF(att.e))))
        case IfThenElse(t, e1, e2, e3) if e2.monoidType == BoolType =>
          println("here");
          convertToCNF(MergeMonoid(BoolType, OrMonoid(), MergeMonoid(BoolType, AndMonoid(), e1, e2), MergeMonoid(BoolType, AndMonoid(), Not(e1), e3)))
        case IfThenElse(t, e1, e2, e3)                        =>
          println("e2 " + e2.monoidType); IfThenElse(t, convertToCNF(e1), convertToCNF(e2), convertToCNF(e3))
        case BinaryOperation(t, op, e1, e2)                   => BinaryOperation(t, op, convertToCNF(e1), convertToCNF(e2))
        case z: EmptySet                                      => z
        case z: EmptyBag                                      => z
        case z: EmptyList                                     => z
        case ConsCollectionMonoid(t, m, e)                    => ConsCollectionMonoid(t, m, convertToCNF(e))
        case MergeMonoid(_, OrMonoid(), BoolConst(true), _)   => BoolConst(true)
        case MergeMonoid(_, OrMonoid(), _, BoolConst(true))   => BoolConst(true)
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
        case Not(e) => Not(convertToCNF(e))
        case Comprehension(t, m, e, gs, pred) => Comprehension(t, m, convertToCNF(e), gs, convertToCNF(pred))
      }
    }

    e match {
      case normalizer.Null()                         => Null()
      case normalizer.BoolConst(v)                   => BoolConst(v)
      case normalizer.StringConst(v)                 => StringConst(v)
      case normalizer.FloatConst(v)                  => FloatConst(v)
      case normalizer.IntConst(v)                    => IntConst(v)
      case v: normalizer.Variable                    => Variable(v)
      case normalizer.RecordProjection(t, e, name)   => RecordProjection(t, apply(e), name)
      case normalizer.RecordConstruction(t, atts)    => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, apply(att.e))))
      case normalizer.IfThenElse(t, e1, e2, e3)      => IfThenElse(t, apply(e1), apply(e2), apply(e3))
      case normalizer.BinaryOperation(t, op, e1, e2) => BinaryOperation(t, op, apply(e1), apply(e2))
      case normalizer.EmptySet()                     => EmptySet()
      case normalizer.EmptyBag()                     => EmptyBag()
      case normalizer.EmptyList()                    => EmptyList()
      case normalizer.ConsCollectionMonoid(t, m, e)  => ConsCollectionMonoid(t, m, apply(e))
      case normalizer.MergeMonoid(t, m, e1, e2)      => MergeMonoid(t, m, apply(e1), apply(e2))
      case normalizer.Comprehension(t, m, e, qs) => {
        val gs = qs.collect { case normalizer.Generator(v: normalizer.Variable, e) => Generator(Variable(v), convertToPath(apply(e))) }
        val ps = qs.collect { case e: normalizer.TypedExpression => apply(e) }
        if (ps.isEmpty)
          Comprehension(t, m, apply(e), gs, BoolConst(true))
        else {
          /* Merge predicates into one expression to convert it to CNF and then split the results again. */
          val head = ps.head
          val rest = ps.drop(1)
          val pred = rest.foldLeft(head)((a, b) => MergeMonoid(BoolType, AndMonoid(), a, b))
          Comprehension(t, m, apply(e), gs, convertToCNF(pred))
        }
      }
      case normalizer.Not(e)           => Not(apply(e))
      case normalizer.FloatToInt(e)    => FloatToInt(apply(e))
      case normalizer.FloatToString(e) => FloatToString(apply(e))
      case normalizer.IntToFloat(e)    => IntToFloat(apply(e))
      case normalizer.IntToString(e)   => IntToString(apply(e))
      case normalizer.StringToBool(e)  => StringToBool(apply(e))
      case normalizer.StringToInt(e)   => StringToInt(apply(e))
      case normalizer.StringToFloat(e) => StringToFloat(apply(e))
    }
  }
}