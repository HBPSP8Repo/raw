package raw.calculus

import raw.RawException

case class CanonizerError(err: String) extends RawException

trait Canonizer extends Normalizer {

  import SymbolTable.{GenVar, ClassEntity}

  /** Convert comprehension into canonical form.
    * The comprehension is first normalized.
    *
    * The canonical form is described in [1], page 19.
    */
  def canonize(c: Calculus.Comp): CanonicalCalculus.Comp = {

    /** Map of symbols to canonical calculus variables. */
    var varMap = scala.collection.mutable.HashMap[GenVar, CanonicalCalculus.Var]()

    def toPath(e: Calculus.Exp): CanonicalCalculus.Path = e match {
      case Calculus.IdnExp(name)       => name -> entity match {
        case _: GenVar => CanonicalCalculus.BoundVar(idnToVar(name))
        case ClassEntity(name, tipe) => CanonicalCalculus.ClassExtent(name)
      }
      case Calculus.RecordProj(e, idn) => CanonicalCalculus.InnerPath(toPath(e), idn)
      case _                           => throw CanonizerError(s"Unexpected expression in path: $e")
    }

    def idnToVar(idn: Calculus.IdnNode): CanonicalCalculus.Var = idn -> entity match {
      case g: GenVar => varMap.get(g) match {
        case Some(v) => v
        case None => {
          val freshVar = CanonicalCalculus.Var()
          varMap.put(g, freshVar)
          freshVar
        }
      }
    }

    def apply(e: Calculus.Exp): CanonicalCalculus.Exp = e match {
      case _: Calculus.Null                    => CanonicalCalculus.Null()
      case Calculus.BoolConst(v)               => CanonicalCalculus.BoolConst(v)
      case Calculus.IntConst(v)                => CanonicalCalculus.IntConst(v)
      case Calculus.FloatConst(v)              => CanonicalCalculus.FloatConst(v)
      case Calculus.StringConst(v)             => CanonicalCalculus.StringConst(v)
      case Calculus.IdnExp(idn)                => idnToVar(idn)
      case Calculus.RecordProj(e, idn)         => CanonicalCalculus.RecordProj(apply(e), idn)
      case Calculus.RecordCons(atts)           => CanonicalCalculus.RecordCons(atts.map { case Calculus.AttrCons(idn, e) => CanonicalCalculus.AttrCons(idn, apply(e))})
      case Calculus.IfThenElse(e1, e2, e3)     => CanonicalCalculus.IfThenElse(apply(e1), apply(e2), apply(e3))
      case Calculus.UnaryExp(op, e)            => CanonicalCalculus.UnaryExp(op, apply(e))
      case Calculus.BinaryExp(op, e1, e2)      => CanonicalCalculus.BinaryExp(op, apply(e1), apply(e2))
      case Calculus.ZeroCollectionMonoid(m)    => CanonicalCalculus.ZeroCollectionMonoid(m)
      case Calculus.ConsCollectionMonoid(m, e) => CanonicalCalculus.ConsCollectionMonoid(m, apply(e))
      case Calculus.MergeMonoid(m, e1, e2)     => CanonicalCalculus.MergeMonoid(m, apply(e1), apply(e2))
      case Calculus.Comp(m, qs, e) => {
        val gs = qs.collect { case g: Calculus.Gen => g}
        val paths = gs.map { case Calculus.Gen(idn, e) => CanonicalCalculus.Gen(idnToVar(idn), toPath(e))}
        val preds = qs.collect { case e: Calculus.Exp => apply(e)}
        CanonicalCalculus.Comp(m, paths.toList, preds.toList, apply(e))
      }
      case _: Calculus.FunAbs | _: Calculus.FunApp => throw CanonizerError(s"Unexpected expression: $e")
    }

    apply(normalize(c)) match {
      case c: CanonicalCalculus.Comp => c
      case e                         => throw CanonizerError(s"Invalid output expression: $e")
    }
  }

}