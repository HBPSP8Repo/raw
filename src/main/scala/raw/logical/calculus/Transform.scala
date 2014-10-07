/**
 * Convert a (normalized) expression into a (canonical) expression.
 *
 * According to [1], the canonical form is defined as:
 *   [...] all generator domains have been reduced to paths and all
 *   predicates have been collected to the right of the comprehension
 *   into pred by anding them together (pred set to true if no predicate
 *   exists).
 */
package raw.logical.calculus

import raw._
import raw.logical._

object Transform {

  def apply(e: parser.TypedExpression): TypedExpression = {

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
      case _ : parser.FunctionType => throw RawInternalException("unexpected FunctionType: expression not normalized")
    }

    def convertVariable(v: parser.Variable) = {
      Variable(convertType(v.parserType), v.name, v.hashCode().toString())
    }
      
    def convertBinaryOperator(op: parser.BinaryOperator) = op match {
      case _ : parser.Eq => Eq
      case _ : parser.Neq => Neq
      case _ : parser.Ge => Ge
      case _ : parser.Gt => Gt
      case _ : parser.Le => Le
      case _ : parser.Lt => Lt
      case _ : parser.Add => Add
      case _ : parser.Sub => Sub
      case _ : parser.Mult => Mult
      case _ : parser.Div => Div
    }
    
    def convertPrimitiveMonoid(m: parser.PrimitiveMonoid): PrimitiveMonoid = m match {
      case _ : parser.SumMonoid => SumMonoid
      case _ : parser.MultiplyMonoid => MultiplyMonoid
      case _ : parser.MaxMonoid => MaxMonoid
      case _ : parser.OrMonoid => OrMonoid
      case _ : parser.AndMonoid => AndMonoid
    }
    
    def convertCollectionMonoid(m: parser.CollectionMonoid): CollectionMonoid = m match {
      case _ : parser.SetMonoid => SetMonoid
      case _ : parser.BagMonoid => BagMonoid
      case _ : parser.ListMonoid => ListMonoid
    }
    
    def convertMonoid(m: parser.Monoid): Monoid = m match {
      case p : parser.PrimitiveMonoid => convertPrimitiveMonoid(p)
      case c : parser.CollectionMonoid => convertCollectionMonoid(c)
    }
    
    /**
     * Convert a sequence of record projections ending in a variable into a Path object.
     */
    def convertToPath(e: Expression): Path = e match {
      case v: Variable                  => VariablePath(v)
      case RecordProjection(t, e, name) => InnerPath(convertToPath(e), name)
      case _                            => throw RawInternalException("unexpected expression in rhs of Generator")
    }
    
    /**
     * Convert an expression into CNF form.
     * A classical but rather inefficient algorithm is used.
     *  
     * 'IfThenElse' where the 'Then' and 'Else' are booleans are also converted to CNF.
     *
     * TODO: MergeMonoid(t, OrMonoid, e1, e2) is inefficient if e1 and e2 are dijunctions of large numbers of literals.
     *       There are more efficient methods but require introducing new variables.
     */
    def convertToCNF(e: TypedExpression): TypedExpression = {
      /**
       * Flatten a predicate in CNF form into a list of predicates.
       */
      def flatten(e: TypedExpression): List[TypedExpression] = e match {
        case MergeMonoid(t, AndMonoid, e1, e2) => flatten(e1) ::: flatten(e2)
        case _                                   => List(e)
      }

      e match {
        case Null                         => Null
        case c: Constant                  => c
        case v: Variable                  => v
        case RecordProjection(t, e, name) => RecordProjection(t, convertToCNF(e), name)
        case RecordConstruction(t, atts)  => RecordConstruction(t, atts.map(att => AttributeConstruction(att.name, convertToCNF(att.e))))
        case IfThenElse(t, e1, e2, e3) if e2.monoidType == BoolType =>
          println("here");
          convertToCNF(MergeMonoid(BoolType, OrMonoid, MergeMonoid(BoolType, AndMonoid, e1, e2), MergeMonoid(BoolType, AndMonoid, Not(e1), e3)))
        case IfThenElse(t, e1, e2, e3)                        =>
          println("e2 " + e2.monoidType); IfThenElse(t, convertToCNF(e1), convertToCNF(e2), convertToCNF(e3))
        case BinaryOperation(t, op, e1, e2)                   => BinaryOperation(t, op, convertToCNF(e1), convertToCNF(e2))
        case EmptySet                                         => EmptySet
        case EmptyBag                                         => EmptyBag
        case EmptyList                                        => EmptyList
        case ConsCollectionMonoid(t, m, e)                    => ConsCollectionMonoid(t, m, convertToCNF(e))
        case MergeMonoid(_, OrMonoid, BoolConst(true), _)   => BoolConst(true)
        case MergeMonoid(_, OrMonoid, _, BoolConst(true))   => BoolConst(true)
        case MergeMonoid(t, OrMonoid, BoolConst(false), e2) => convertToCNF(e2)
        case MergeMonoid(t, OrMonoid, e1, BoolConst(false)) => convertToCNF(e1)
        case MergeMonoid(t, OrMonoid, e1, e2) => {
          val p = convertToCNF(e1)
          val q = convertToCNF(e2)
          val ps = flatten(p)
          val qs = flatten(q)
          val prod = for (p <- ps; q <- qs) yield MergeMonoid(BoolType, OrMonoid, p, q)
          val head = prod.head
          val rest = prod.drop(1)
          convertToCNF(rest.foldLeft(head)((a, b) => MergeMonoid(BoolType, AndMonoid, a, b)))
        }
        case MergeMonoid(_, AndMonoid, BoolConst(false), _) => BoolConst(false)
        case MergeMonoid(_, AndMonoid, _, BoolConst(false)) => BoolConst(false)
        case MergeMonoid(t, AndMonoid, BoolConst(true), e2) => convertToCNF(e2)
        case MergeMonoid(t, AndMonoid, e1, BoolConst(true)) => convertToCNF(e1)
        case MergeMonoid(t, m, e1, e2) => MergeMonoid(t, m, convertToCNF(e1), convertToCNF(e2))
        case Not(Not(e)) => convertToCNF(e)
        case Not(MergeMonoid(t, AndMonoid, e1, e2)) => convertToCNF(MergeMonoid(t, OrMonoid, Not(e1), Not(e2)))
        case Not(MergeMonoid(t, OrMonoid, e1, e2)) => convertToCNF(MergeMonoid(t, AndMonoid, Not(e1), Not(e2)))
        case Not(e) => Not(convertToCNF(e))
        case Comprehension(t, m, e, gs, pred) => Comprehension(t, m, convertToCNF(e), gs, convertToCNF(pred))
      }
    }
    
    e match {
      case parser.Null() => Null
      case parser.BoolConst(v) => BoolConst(v)
      case parser.StringConst(v) => StringConst(v)
      case parser.FloatConst(v) => FloatConst(v)
      case parser.IntConst(v) => IntConst(v)
      case v: parser.Variable => convertVariable(v)
      case parser.RecordProjection(t, e, name) => RecordProjection(convertType(t), apply(e), name)
      case parser.RecordConstruction(t, atts) => RecordConstruction(convertType(t), atts.map(att => AttributeConstruction(att.name, apply(att.e))))
      case parser.IfThenElse(t, e1, e2, e3) => IfThenElse(convertType(t), apply(e1), apply(e2), apply(e3))
      case parser.BinaryOperation(t, op, e1, e2) => BinaryOperation(convertType(t), convertBinaryOperator(op), apply(e1), apply(e2))
      case _: parser.FunctionAbstraction => throw RawInternalException("unexpected FunctionAbstraction: expression not normalized")
      case _: parser.FunctionApplication => throw RawInternalException("unexpected FunctionApplication: expression not normalized")
      case parser.EmptySet() => EmptySet
      case parser.EmptyBag() => EmptyBag
      case parser.EmptyList() => EmptyList
      case parser.ConsCollectionMonoid(t, m, e) => ConsCollectionMonoid(convertType(t), convertCollectionMonoid(m), apply(e))
      case parser.MergeMonoid(t, m, e1, e2) => MergeMonoid(convertType(t), convertMonoid(m), apply(e1), apply(e2))
      case parser.Comprehension(t, m, e, qs) => {
        val gs = qs.collect { case parser.Generator(v: parser.Variable, e) => Generator(convertVariable(v), convertToPath(apply(e))) }
        val ps = qs.collect { case e: parser.TypedExpression => apply(e) }
        if ((ps.length + gs.length) != qs.length)
          throw RawInternalException("unexpected expression: expression not normalized")
        else if (ps.isEmpty)
          Comprehension(convertType(t), convertMonoid(m), apply(e), gs, BoolConst(true))
        else {
          /* Merge predicates into one expression to convert it to CNF and then split the results again. */
          val head = ps.head
          val rest = ps.drop(1)
          val pred = rest.foldLeft(head)((a, b) => MergeMonoid(BoolType, AndMonoid, a, b))
          Comprehension(convertType(t), convertMonoid(m), apply(e), gs, convertToCNF(pred))
        }
      }
      case parser.Not(e) => Not(apply(e))
      case parser.FloatToInt(e) => FloatToInt(apply(e))
      case parser.FloatToString(e) => FloatToString(apply(e))
      case parser.IntToFloat(e) => IntToFloat(apply(e))
      case parser.IntToString(e) => IntToString(apply(e))
      case parser.StringToBool(e) => StringToBool(apply(e))
      case parser.StringToInt(e) => StringToInt(apply(e))
      case parser.StringToFloat(e) => StringToFloat(apply(e))
    }
  }
}
