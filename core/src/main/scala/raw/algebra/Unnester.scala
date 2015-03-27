package raw
package algebra

import com.typesafe.scalalogging.LazyLogging
import org.kiama.util.Entity
import raw.calculus.Calculus

case class UnnesterError(err: String) extends RawException(err)

/** Patterns
  */
sealed abstract class Pattern
case class PairPattern(fst: Pattern, snd: Pattern) extends Pattern
case class IdnPattern(idn: String, t: Type) extends Pattern

/** Terms used during query unnesting.
  */
sealed abstract class Term
case object EmptyTerm extends Term
case class CalculusTerm(c: Calculus.Exp, u: Option[Pattern], w: Option[Pattern], child: Term) extends Term
case class AlgebraTerm(t: LogicalAlgebra.LogicalAlgebraNode) extends Term

/** Algorithm that converts a calculus expression (in canonical form) into the logical algebra.
  * The algorithm is described in Fig. 10 of [1], page 34.
  */
object Unnester extends LazyLogging {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import org.kiama.rewriting.Strategy
  import raw.calculus.{SemanticAnalyzer, Simplifier, SymbolTable}

  def apply(tree: Calculus.Calculus, world: World): LogicalAlgebra.LogicalAlgebraNode = {

    /** Main execution:
      * TODO: turn into an attribute?
      */

    val t = tree
    val w = world
    val s = new Simplifier {
      val tree = t;
      val world = w
    }

    val inTree = s.transform

    val analyzer = new SemanticAnalyzer {
      val world = w;
      val tree = inTree
    }

    val collectEntities = collect[List, Tuple2[String, Entity]]{ case Calculus.IdnExp(idn) => idn.idn -> analyzer.entity(idn) }
    val entities = collectEntities(inTree.root).toMap

    val collectTypes = collect[List, Tuple2[Calculus.Exp, Type]]{ case e: Calculus.Exp => e -> analyzer.tipe(e) }
    val tipes = collectTypes(inTree.root).toMap

    logger.debug(s"Unnester input tree: ${calculus.CalculusPrettyPrinter(inTree.root)}")

    def unnest(e: Calculus.Exp): LogicalAlgebra.LogicalAlgebraNode = {

      /** Extract object to pattern match scan objects.
        */
      object Scan {
        def unapply(e: Calculus.Exp): Option[LogicalAlgebra.Scan] = e match {
          case Calculus.IdnExp(idn) => entities(idn.idn) match {
            case e@SymbolTable.DataSourceEntity(name) => Some(LogicalAlgebra.Scan(name, world.sources(name))) // TODO: Use more precise type if one was inferred
            case _ => None
          }
          case _                    => None
        }
      }

      /** Extract object to pattern match comprehensions in the canonical form.
        */
      object CanonicalComp {

        import Calculus._

        def apply(m: Monoid, paths: List[Gen], preds: List[Exp], e: Exp): Comp =
          Comp(m, paths ++ preds, e)

        def unapply(c: Comp): Option[(Monoid, List[Gen], List[Exp], Exp)] = {
          val paths = c.qs.collect { case g: Gen => g }
          val preds = c.qs.collect { case e: Exp => e }
          Some(c.m, paths.toList, preds.toList, c.e)
        }
      }

      /** Extractor object to pattern match nested comprehensions.
        */
      object NestedComp {

        import Calculus._

        def unapply(e: Exp): Option[Comp] = e match {
          case RecordProj(NestedComp(e1), _)           => Some(e1)
          case RecordCons(atts)                        => atts.map(att => att.e match {
            case NestedComp(e1) => Some(e1)
            case _              => None
          }).flatten.headOption
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

      /** Return the inner type of a collection.
        */
      def getInnerType(t: Type): Type = t match {
        case c: CollectionType => c.innerType
        case UserType(idn)     => getInnerType(world.userTypes(idn))
        case _                 => throw UnnesterError(s"Expected collection type but got $t")
      }

      /** Return the sequence of identifiers used in a pattern.
        */
      def getIdns(p: Pattern): Seq[String] = p match {
        case PairPattern(p1, p2) => getIdns(p1) ++ getIdns(p2)
        case IdnPattern(p1, _)   => Seq(p1)
      }

      /** Return the set of variable used in an expression.
        */
      def variables(e: Calculus.Exp): Set[String] = {
        val collectIdns = collect[Set, String]{ case Calculus.IdnExp(idn) => idn.idn }
        collectIdns(e)
      }

      /** Build type from pattern.
        */
      def patternType(p: Pattern): Type = p match {
        case PairPattern(p1, p2) => RecordType(List(AttrType("_1", patternType(p1)), AttrType("_2", patternType(p2))))
        case IdnPattern(p1, t)   => t
      }

      /** Build algebra expression that projects the identifier given the pattern.
        */
      def buildPatternExp(idn: String, p: Pattern): Expressions.Exp = {
        def recurse(p: Pattern, e: Expressions.Exp): Option[Expressions.Exp] = p match {
          case IdnPattern(idn1, _) if idn == idn1 => Some(e)
          case PairPattern(fst, snd) =>
            recurse(fst, Expressions.RecordProj(e, "_1")) match {
              case Some(e1) => Some(e1)
              case _ => recurse(snd, Expressions.RecordProj(e, "_2")) match {
                case Some(e2) => Some(e2)
                case _        => None
              }
            }
          case _                     => None
        }

        recurse(p, Expressions.Arg(patternType(p))).head
      }

      /** Convert canonical calculus expression to algebra expression.
        * The position of each canonical expression variable is used as the argument.
        */
      def createExp(e: Calculus.Exp, p: Pattern): Expressions.Exp = {
        def recurse(e: Calculus.Exp): Expressions.Exp = e match {
          case _: Calculus.Null                                 => Expressions.Null
          case Calculus.BoolConst(v)                            => Expressions.BoolConst(v)
          case Calculus.IntConst(v)                             => Expressions.IntConst(v)
          case Calculus.FloatConst(v)                           => Expressions.FloatConst(v)
          case Calculus.StringConst(v)                          => Expressions.StringConst(v)
          case Calculus.IdnExp(idn)                             => buildPatternExp(idn.idn, p)
          case Calculus.RecordProj(e1, idn)                     => Expressions.RecordProj(recurse(e1), idn)
          case Calculus.RecordCons(atts)                        => Expressions.RecordCons(atts.map { att => Expressions.AttrCons(att.idn, recurse(att.e)) })
          case Calculus.IfThenElse(e1, e2, e3)                  => Expressions.IfThenElse(recurse(e1), recurse(e2), recurse(e3))
          case Calculus.BinaryExp(op, e1, e2)                   => Expressions.BinaryExp(op, recurse(e1), recurse(e2))
          case Calculus.MergeMonoid(m: PrimitiveMonoid, e1, e2) => Expressions.MergeMonoid(m, recurse(e1), recurse(e2))
          case Calculus.UnaryExp(op, e1)                        => Expressions.UnaryExp(op, recurse(e1))
          case n                                                => throw UnnesterError(s"Unexpected node: $n")
        }

        recurse(e)
      }

      /** Create predicate, which is an expression in conjunctive normal form.
        */
      def createPredicate(ps: Seq[Calculus.Exp], pat: Pattern): Expressions.Exp = {
        ps match {
          case Nil          => Expressions.BoolConst(true)
          case head :: Nil  => createExp(head, pat)
          case head :: tail => tail.map(createExp(_, pat)).foldLeft(createExp(head, pat))((a, b) => Expressions.MergeMonoid(AndMonoid(), a, b))
        }
      }

      /** Create a record projection (for Nest's group-by and nulls)
        */
      def createRecord(idns: Seq[String], pat: Pattern): Expressions.Exp =
        if (idns.length == 1)
          buildPatternExp(idns.head, pat)
        else
          Expressions.RecordCons(idns.zipWithIndex.map { case (idn, idx) => Expressions.AttrCons(s"_${idx + 1}", buildPatternExp(idn, pat)) })

      /** Returns true if the comprehension `c` does not depend on `s` generators.
        */
      def areIndependent(c: Calculus.Comp, s: List[Calculus.Gen]) = {
        val sVs: Set[String] = s.map { case Calculus.Gen(Calculus.IdnDef(v, _), _) => v }.toSet
        variables(c).intersect(sVs).isEmpty
      }

      // TODO: hasNestedComp followed by getNestedComp is *slow* code; replace by extractor
      def hasNestedComp(ps: List[Calculus.Exp]) =
        ps.collectFirst { case NestedComp(c) => c }.isDefined

      def getNestedComp(ps: List[Calculus.Exp]) =
        ps.collectFirst { case NestedComp(c) => c }.head

      def freshIdn = SymbolTable.next()

      def apply(t: Term): Term =
        t match {

        /** Rule C11.
          */
        case CalculusTerm(CanonicalComp(m, s, p, e1), u, Some(w), child) if hasNestedComp(p) && areIndependent(getNestedComp(p), s) =>
          logger.debug(s"Applying unnester rule C11")
          val c = getNestedComp(p)
          val v = freshIdn
          val pat_v = IdnPattern(v, tipes(c))
          val pat_w_v = PairPattern(w, pat_v)
          val npred = p.map(rewrite(attempt(oncetd(rule[Calculus.Exp] {
            case `c` => Calculus.IdnExp(Calculus.IdnUse(v))
          })))(_))
          apply(CalculusTerm(CanonicalComp(m, s, npred, e1), u, Some(pat_w_v), apply(CalculusTerm(c, Some(w), Some(w), child))))

        /** Rule C12.
          */
        case CalculusTerm(CanonicalComp(m, Nil, p, f@NestedComp(c)), u, Some(w), child) =>
          logger.debug(s"Applying unnester rule C12")
          val v = freshIdn
          val pat_v = IdnPattern(v, tipes(c))
          val pat_w_v = PairPattern(w, pat_v)
          val nf = rewrite(oncetd(rule[Calculus.Exp] {
            case `c` => Calculus.IdnExp(Calculus.IdnUse(v))
          }))(f)
          apply(CalculusTerm(CanonicalComp(m, Nil, p, nf), u, Some(pat_w_v), apply(CalculusTerm(c, Some(w), Some(w), child))))

        /** Rule C4.
          */
        case CalculusTerm(CanonicalComp(m, Calculus.Gen(Calculus.IdnDef(v, _), Scan(x)) :: r, p, e), None, None, EmptyTerm) =>
          logger.debug(s"Applying unnester rule C4")
          val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
          val pat_v = IdnPattern(v, getInnerType(x.t))
          apply(CalculusTerm(CanonicalComp(m, r, pred_not_v, e), None, Some(pat_v), AlgebraTerm(LogicalAlgebra.Select(createPredicate(pred_v, pat_v), x))))

        /** Rule C5
          */

        case CalculusTerm(CanonicalComp(m, Nil, p, e), None, Some(w), AlgebraTerm(child)) =>
          logger.debug(s"Applying unnester rule C5")
          AlgebraTerm(LogicalAlgebra.Reduce(m, createExp(e, w), createPredicate(p, w), child))

        /** Rule C6
          */

        case CalculusTerm(CanonicalComp(m, Calculus.Gen(Calculus.IdnDef(v, _), Scan(x)) :: r, p, e), None, Some(w), AlgebraTerm(child)) =>
          logger.debug(s"Applying unnester rule C6")
          val pat_v = IdnPattern(v, getInnerType(x.t))
          val pat_w_v = PairPattern(w, pat_v)
          val pred_v = p.filter(variables(_) == Set(v))
          val pred_w_v = p.filter(pred => !pred_v.contains(pred) && variables(pred).subsetOf(getIdns(pat_w_v).toSet))
          val pred_rest = p.filter(pred => !pred_v.contains(pred) && !pred_w_v.contains(pred))
          apply(CalculusTerm(CanonicalComp(m, r, pred_rest, e), None, Some(pat_w_v), AlgebraTerm(LogicalAlgebra.Join(createPredicate(pred_w_v, pat_w_v), child, LogicalAlgebra.Select(createPredicate(pred_v, pat_v), x)))))

        /** Rule C7
          */

        case CalculusTerm(CanonicalComp(m, Calculus.Gen(Calculus.IdnDef(v, _), path) :: r, p, e), None, Some(w), AlgebraTerm(child)) =>
          logger.debug(s"Applying unnester rule C7")
          val pat_v = IdnPattern(v, getInnerType(tipes(path)))
          val pat_w_v = PairPattern(w, pat_v)
          val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
          apply(CalculusTerm(CanonicalComp(m, r, pred_not_v, e), None, Some(pat_w_v), AlgebraTerm(LogicalAlgebra.Unnest(createExp(path, w), createPredicate(pred_v, pat_w_v), child))))

        /** Rule C8
          */

        case CalculusTerm(CanonicalComp(m, Nil, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
          logger.debug(s"Applying unnester rule C8")
          AlgebraTerm(LogicalAlgebra.Nest(m, createExp(e, w), createRecord(getIdns(u), w), createPredicate(p, w), createRecord(getIdns(w).filterNot(getIdns(u).contains), w), child))

        /** Rule C9
          */

        case CalculusTerm(CanonicalComp(m, Calculus.Gen(Calculus.IdnDef(v, _), Scan(x)) :: r, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
          logger.debug(s"Applying unnester rule C9")
          val pat_v = IdnPattern(v, getInnerType(x.t))
          val pat_w_v = PairPattern(w, pat_v)
          val pred_v = p.filter(variables(_) == Set(v))
          val pred_w_v = p.filter(pred => getIdns(pat_w_v).toSet.subsetOf(variables(pred)))
          val pred_rest = p.filter(pred => !pred_v.contains(pred) && !pred_w_v.contains(pred))
          apply(CalculusTerm(CanonicalComp(m, r, pred_rest, e), Some(u), Some(pat_w_v), AlgebraTerm(LogicalAlgebra.OuterJoin(createPredicate(pred_w_v, pat_w_v), child, LogicalAlgebra.Select(createPredicate(pred_v, pat_v), x)))))

        /** Rule C10
          */

        case CalculusTerm(CanonicalComp(m, Calculus.Gen(Calculus.IdnDef(v, _), path) :: r, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
          logger.debug(s"Applying unnester rule C10")
          val pat_v = IdnPattern(v, getInnerType(tipes(path)))
          val pat_w_v = PairPattern(w, pat_v)
          val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
          apply(CalculusTerm(CanonicalComp(m, r, pred_not_v, e), Some(u), Some(pat_w_v), AlgebraTerm(LogicalAlgebra.OuterUnnest(createExp(path, w), createPredicate(pred_v, pat_w_v), child))))

        /** Rule (missing from the paper) to handle merge of two comprehensions
          */

        case CalculusTerm(Calculus.MergeMonoid(m, e1, e2), None, None, EmptyTerm) =>
          logger.debug(s"Applying unnester rule TopLevelMerge")
          AlgebraTerm(LogicalAlgebra.Merge(m, unnest(e1), unnest(e2)))

      }

      apply(CalculusTerm(e, None, None, EmptyTerm)) match {
        case AlgebraTerm(a) => a
        case o              => throw UnnesterError(s"Invalid output: $o")
      }
    }

    unnest(inTree.root)
  }
}