package raw
package calculus

import Calculus._

case class UnnesterError(err: String) extends RawException(err)

/** Terms used during query unnesting.
  */
sealed abstract class Term
case object EmptyTerm extends Term
case class CalculusTerm(c: Calculus.Exp, u: Option[Pattern], w: Option[Pattern], child: Term) extends Term
case class AlgebraTerm(t: LogicalAlgebraNode) extends Term

/** Algorithm that converts a calculus expression (in canonical form) into the logical algebra.
  * The algorithm is described in Fig. 10 of [1], page 34.
  */
trait Unnester extends Simplifier {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._

  override def strategy = attempt(super.strategy) <* unnester

  lazy val unnester = reduce(unnest)

  lazy val unnest = rule[Exp] {
    case c: Comp =>
      recurse(CalculusTerm(c, None, None, EmptyTerm)) match {
        case AlgebraTerm(a) => a
        case o              => throw UnnesterError(s"Invalid output: $o")
      }
    }

  def recurse(t: Term): Term =
    t match {
      /** Rule C11.
        */
      case CalculusTerm(CanonicalComp(m, s, p, e1), u, Some(w), child) if hasNestedComp(p) && areIndependent(getNestedComp(p), s) =>
        logger.debug(s"Applying unnester rule C11")
        val c = getNestedComp(p)
        val v = SymbolTable.next()
        val pat_v = PatternIdn(IdnDef(v.idn))
        val pat_w_v = PatternProd(Seq(w, pat_v))
        val npred = p.map(rewrite(attempt(oncetd(rule[Exp] {
          case `c` => IdnExp(IdnUse(v.idn))
        })))(_))
        recurse(CalculusTerm(CanonicalComp(m, s, npred, e1), u, Some(pat_w_v), recurse(CalculusTerm(c, Some(w), Some(w), child))))

      /** Rule C12.
        */
      case CalculusTerm(CanonicalComp(m, Nil, p, f@NestedComp(c)), u, Some(w), child) =>
        logger.debug(s"Applying unnester rule C12")
        val v = SymbolTable.next()
        val pat_v = PatternIdn(IdnDef(v.idn))
        val pat_w_v = PatternProd(Seq(w, pat_v))
        val nf = rewrite(oncetd(rule[Exp] {
          case `c` => IdnExp(IdnUse(v.idn))
        }))(f)
        recurse(CalculusTerm(CanonicalComp(m, Nil, p, nf), u, Some(pat_w_v), recurse(CalculusTerm(c, Some(w), Some(w), child))))

      /** Rule C4.
        */
      case CalculusTerm(CanonicalComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), x: IdnExp) :: r, p, e), None, None, EmptyTerm) =>
        logger.debug(s"Applying unnester rule C4")
        val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
        recurse(CalculusTerm(CanonicalComp(m, r, pred_not_v, e), None, Some(pat_v), AlgebraTerm(Filter(FunAbs(pat_v, foldPreds(pred_v)), x))))

      /** Rule C5
        */

      case CalculusTerm(CanonicalComp(m, Nil, p, e), None, Some(w), AlgebraTerm(child)) =>
        logger.debug(s"Applying unnester rule C5")
        AlgebraTerm(Reduce(m, FunAbs(w, e), FunAbs(w, foldPreds(p)), child))

      /** Rule C6
        */

      case CalculusTerm(CanonicalComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), x: IdnExp) :: r, p, e), None, Some(w), AlgebraTerm(child)) =>
        logger.debug(s"Applying unnester rule C6")
        val pat_w_v = PatternProd(Seq(w, pat_v))
        val pred_v = p.filter(variables(_) == Set(v))
        val pred_w_v = p.filter(pred => !pred_v.contains(pred) && variables(pred).subsetOf(getIdns(pat_w_v).toSet))
        val pred_rest = p.filter(pred => !pred_v.contains(pred) && !pred_w_v.contains(pred))
        recurse(CalculusTerm(CanonicalComp(m, r, pred_rest, e), None, Some(pat_w_v), AlgebraTerm(Join(FunAbs(pat_w_v, foldPreds(pred_w_v)), child, Filter(FunAbs(pat_v, foldPreds(pred_v)), x)))))

      /** Rule C7
        */

      case CalculusTerm(CanonicalComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), path) :: r, p, e), None, Some(w), AlgebraTerm(child)) =>
        logger.debug(s"Applying unnester rule C7")
        val pat_w_v = PatternProd(Seq(w, pat_v))
        val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
        recurse(CalculusTerm(CanonicalComp(m, r, pred_not_v, e), None, Some(pat_w_v), AlgebraTerm(Unnest(FunAbs(w, path), FunAbs(pat_w_v, foldPreds(pred_v)), child))))

      /** Rule C8
        */

      case CalculusTerm(CanonicalComp(m, Nil, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
        logger.debug(s"Applying unnester rule C8")
        AlgebraTerm(Nest(m, FunAbs(w, e), createRecord(u, w), FunAbs(w, foldPreds(p)), createRecord(subtractPattern(w, u), w), child))

      /** Rule C9
        */

      case CalculusTerm(CanonicalComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), x: IdnExp) :: r, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
        logger.debug(s"Applying unnester rule C9")
        val pat_w_v = PatternProd(Seq(w, pat_v))
        val pred_v = p.filter(variables(_) == Set(v))
        val pred_w_v = p.filter(pred => !pred_v.contains(pred) && variables(pred).subsetOf(getIdns(pat_w_v).toSet))
        val pred_rest = p.filter(pred => !pred_v.contains(pred) && !pred_w_v.contains(pred))
        recurse(CalculusTerm(CanonicalComp(m, r, pred_rest, e), Some(u), Some(pat_w_v), AlgebraTerm(OuterJoin(FunAbs(pat_w_v, foldPreds(pred_w_v)), child, Filter(FunAbs(pat_v, foldPreds(pred_v)), x)))))

      /** Rule C10
        */

      case CalculusTerm(CanonicalComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), path) :: r, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
        logger.debug(s"Applying unnester rule C10")
        val pat_w_v = PatternProd(Seq(w, pat_v))
        val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
        recurse(CalculusTerm(CanonicalComp(m, r, pred_not_v, e), Some(u), Some(pat_w_v), AlgebraTerm(OuterUnnest(FunAbs(w, path), FunAbs(pat_w_v, foldPreds(pred_v)), child))))
    }

  def foldPreds(ps: Seq[Exp]) = ps.foldLeft(BoolConst(true).asInstanceOf[Exp])((a, b) => MergeMonoid(AndMonoid(), a, b))



  /** Build an expression that projects the identifier given the pattern.
    */
  def projectPattern(p: Pattern, idn: IdnDef): Exp = {
    def recurse(p: Pattern): Option[Exp] = p match {
      case PatternIdn(`idn`) => Some(IdnExp(IdnUse(idn.idn)))
      case _: PatternIdn => None
      case PatternProd(ps) =>
        for ((p, i) <- ps.zipWithIndex) {
          if (recurse(p).isDefined) {
            return Some(RecordProj(recurse(p).head, s"_${i + 1}"))
          }
        }
        None
    }

    recurse(p).head
  }

  /** Create a record expression from a pattern (used by Nest).
    */
  def createRecord(u: Pattern, p: Pattern): Exp = u match {
    case PatternProd(ps) => RecordCons(ps.zipWithIndex.map { case (np, i) => AttrCons(s"_${i + 1}", createRecord(np, p))})
    case PatternIdn(idn) => projectPattern(p, idn)
  }

  /** Return a new pattern which corresponds to w minus u (used by Nest).
    */
  def subtractPattern(w: Pattern, u: Pattern): Pattern = {
    def recurse(w: Pattern): Seq[Pattern]  = w match {
      case PatternProd(ps) => ps.flatMap(recurse)
      case `u` => Seq()
      case p   => Seq(p)
    }

    recurse(w) match {
      case p :: Nil => p
      case ps => PatternProd(ps)
    }
  }


  /** Return the sequence of identifiers used in a pattern.
    */
  def getIdns(p: Pattern): Seq[String] = p match {
    case PatternProd(ps) => ps.flatMap(getIdns)
    case PatternIdn(IdnDef(idn))   => Seq(idn)
  }

  /** Returns true if the comprehension `c` does not depend on `s` generators.
    */
  def areIndependent(c: Comp, s: List[Gen]) = {
    val sVs: Set[String] = s.map { case Gen(PatternIdn(IdnDef(v)), _) => v }.toSet
    variables(c).intersect(sVs).isEmpty
  }

  // TODO: hasNestedComp followed by getNestedComp is *slow* code; replace by extractor
  def hasNestedComp(ps: List[Exp]) =
    ps.collectFirst { case NestedComp(c) => c }.isDefined

  def getNestedComp(ps: List[Exp]) =
    ps.collectFirst { case NestedComp(c) => c }.head

  /** Return the set of variable used in an expression.
    */
  def variables(e: Exp): Set[String] = {
    val collectIdns = collect[Set, String]{ case IdnExp(idn) => idn.idn }
    collectIdns(e)
  }

  /** Extractor to pattern match comprehensions in the canonical form.
    */
  private object CanonicalComp {
    def apply(m: Monoid, paths: List[Gen], preds: List[Exp], e: Exp): Comp =
      Comp(m, paths ++ preds, e)

    def unapply(c: Comp): Option[(Monoid, List[Gen], List[Exp], Exp)] = {
      val paths = c.qs.collect { case g: Gen => g }

      def flattenPreds(p: Exp): Seq[Exp] = p match {
        case MergeMonoid(_: AndMonoid, e1, e2) => flattenPreds(e1) ++ flattenPreds(e2)
        case _                                 => Seq(p)
      }
      val preds = c.qs.collect { case p: Exp => p }.flatMap { case p: Exp => flattenPreds(p) }

      Some(c.m, paths.toList, preds.toList, c.e)
    }
  }

  /** Extractor to pattern match nested comprehensions.
    */
  private object NestedComp {
    def unapply(e: Exp): Option[Comp] = e match {
      case RecordProj(NestedComp(e1), _)           => Some(e1)
      case RecordCons(atts)                        => atts.flatMap(att => att.e match {
        case NestedComp(e1) => Some(e1)
        case _              => None
      }).headOption
      case IfThenElse(NestedComp(e1), _, _)        => Some(e1)
      case IfThenElse(_, NestedComp(e2), _)        => Some(e2)
      case IfThenElse(_, _, NestedComp(e3))        => Some(e3)
      case BinaryExp(_, NestedComp(e1), _)         => Some(e1)
      case BinaryExp(_, _, NestedComp(e2))         => Some(e2)
      case ConsCollectionMonoid(_, NestedComp(e1)) => Some(e1)
      case MergeMonoid(_, NestedComp(e1), _)       => Some(e1)
      case MergeMonoid(_, _, NestedComp(e2))       => Some(e2)
      case UnaryExp(_, NestedComp(e1))             => Some(e1)
      case c: Comp                                 => Some(c)
      case _                                       => None
    }
  }
}

object Unnester {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree)
    val a = new SemanticAnalyzer(t1, world)
    val unnester = new Unnester {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(unnester.strategy)(t1)
  }
}


//
//object Unnester extends LazyLogging {
//
//  def apply(tree: Calculus.Calculus, world: World): LogicalAlgebra.LogicalAlgebraNode = {
//    val tree1 = Simplifier(tree, world)
//
//    val analyzer = new SemanticAnalyzer(tree1, world)
//
//    val collectEntities = collect[List, Tuple2[String, Entity]]{ case Calculus.IdnExp(idn) => idn.idn -> analyzer.entity(idn) }
//    val entities = collectEntities(tree1.root).toMap
//
//    val collectTypes = collect[List, Tuple2[Calculus.Exp, Type]]{ case e: Calculus.Exp => e -> analyzer.tipe(e) }
//    val tipes = collectTypes(tree1.root).toMap
//
//    logger.debug(s"Unnester input tree: ${calculus.CalculusPrettyPrinter(tree1.root)}")
//
//    def unnest(e: Calculus.Exp): LogicalAlgebra.LogicalAlgebraNode = {
//
//      /** Extract object to pattern match scan objects.
//        */
//      object Scan {
//        def unapply(e: Calculus.Exp): Option[LogicalAlgebra.Scan] = e match {
//          case Calculus.IdnExp(idn) => entities(idn.idn) match {
//            case e@SymbolTable.DataSourceEntity(sym) => Some(LogicalAlgebra.Scan(sym.idn, world.sources(sym.idn))) // TODO: Use more precise type if one was inferred
//            case _ => None
//          }
//          case _                    => None
//        }
//      }
//
//
//
//      /** Return the inner type of a collection.
//        */
//      def getInnerType(t: Type): Type = t match {
//        case c: CollectionType => c.innerType
//        //        case UserType(idn)     => getInnerType(world.userTypes(idn))
//        case _                 => throw UnnesterError(s"Expected collection type but got $t")
//      }
//
//      /** Return the set of variable used in an expression.
//        */
//      def variables(e: Calculus.Exp): Set[String] = {
//        val collectIdns = collect[Set, String]{ case Calculus.IdnExp(idn) => idn.idn }
//        collectIdns(e)
//      }
//
//      /** Build type from pattern.
//        */
//      def patternType(p: Pattern): Type = p match {
//        case PairPattern(p1, p2) => RecordType(List(AttrType("_1", patternType(p1)), AttrType("_2", patternType(p2))), None)
//        case IdnPattern(p1, t)   => t
//      }
//
//      /** Build algebra expression that projects the identifier given the pattern.
//        */
//      def buildPatternExp(idn: String, p: Pattern): Expressions.Exp = {
//        def recurse(p: Pattern, e: Expressions.Exp): Option[Expressions.Exp] = p match {
//          case IdnPattern(idn1, _) if idn == idn1 => Some(e)
//          case PairPattern(fst, snd) =>
//            recurse(fst, Expressions.RecordProj(e, "_1")) match {
//              case Some(e1) => Some(e1)
//              case _ => recurse(snd, Expressions.RecordProj(e, "_2")) match {
//                case Some(e2) => Some(e2)
//                case _        => None
//              }
//            }
//          case _                     => None
//        }
//
//        recurse(p, Expressions.Arg(patternType(p))).head
//      }
//
//      /** Convert canonical calculus expression to algebra expression.
//        * The position of each canonical expression variable is used as the argument.
//        */
//      def createExp(e: Calculus.Exp, p: Pattern): Expressions.Exp = {
//        def recurse(e: Calculus.Exp): Expressions.Exp = e match {
//          case _: Calculus.Null                                 => Expressions.Null
//          case Calculus.BoolConst(v)                            => Expressions.BoolConst(v)
//          case Calculus.IntConst(v)                             => Expressions.IntConst(v)
//          case Calculus.FloatConst(v)                           => Expressions.FloatConst(v)
//          case Calculus.StringConst(v)                          => Expressions.StringConst(v)
//          case Calculus.IdnExp(idn)                             => buildPatternExp(idn.idn, p)
//          case Calculus.RecordProj(e1, idn)                     => Expressions.RecordProj(recurse(e1), idn)
//          case Calculus.RecordCons(atts)                        => Expressions.RecordCons(atts.map { att => Expressions.AttrCons(att.idn, recurse(att.e)) })
//          case Calculus.IfThenElse(e1, e2, e3)                  => Expressions.IfThenElse(recurse(e1), recurse(e2), recurse(e3))
//          case Calculus.BinaryExp(op, e1, e2)                   => Expressions.BinaryExp(op, recurse(e1), recurse(e2))
//          case Calculus.MergeMonoid(m: PrimitiveMonoid, e1, e2) => Expressions.MergeMonoid(m, recurse(e1), recurse(e2))
//          case Calculus.UnaryExp(op, e1)                        => Expressions.UnaryExp(op, recurse(e1))
//          case n                                                => throw UnnesterError(s"Unexpected node: $n")
//        }
//
//        recurse(e)
//      }
//
//      /** Create a predicate, which is an expression in conjunctive normal form.
//        */
//      def createPredicate(ps: Seq[Calculus.Exp], pat: Pattern): Expressions.Exp = {
//        ps match {
//          case Nil          => Expressions.BoolConst(true)
//          case head :: Nil  => createExp(head, pat)
//          case head :: tail => tail.map(createExp(_, pat)).foldLeft(createExp(head, pat))((a, b) => Expressions.MergeMonoid(AndMonoid(), a, b))
//        }
//      }
//
//
//
//
//      def freshIdn = SymbolTable.next().idn
//
//
//
//    }
//
//    unnest(tree1.root)
//  }
//
//}
//
