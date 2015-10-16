package raw
package calculus

import org.kiama.attribution.Attribution

class GroupByDesugarer(val analyzer: SemanticAnalyzer) extends Attribution with SemanticTransformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._
  import SymbolTable._

  def strategy = desugar

  private lazy val desugar =
    innermost(selectGroupBy)

  /** In the case of a SELECT with GROUP BY, we must desugar partition and *, replacing them by an "inner" SELECT.
    * In the case of partition, we replace by a SELECT whose projection is not flattened, e.g.:
    *
    *   SELECT $0.age, PARTITION
    *   FROM $0 <- students, $1 <- professors WHERE $0.student_age = $1.professor_age GROUP BY $0.student_age
    *
    *   ... is rewritten into ...
    *
    *   SELECT $0.age, (SELECT $2, $3 FROM $2 <- students, $3 <- professors WHERE $2.student_age = $3.professor_age AND $0.student_age = $2.student_age)
    *   FROM $0 <- students, $1 <- professors WHERE $0.student_age = $1.professor_age GROUP BY $0.student_age
    *
    * Conversely, in the case of *, we replace by a SELECT whose project is flattened, e.g.:
    *
    *   SELECT $0.age, *
    *   FROM $0 <- students, $1 <- professors WHERE $0.student_age = $1.professor_age GROUP BY $0.student_age
    *
    *   ... is rewritten into ...
    *
    *   SELECT $0.age, (SELECT * FROM $2 <- students, $3 <- professors WHERE $2.student_age = $3.professor_age AND $0.student_age = $2.student_age)
    *   FROM $0 <- students, $1 <- professors WHERE $0.student_age = $1.professor_age GROUP BY $0.student_age
    */

  private lazy val partitionSelect: Select => Select = attr {
    s =>
      val ns = rewriteIdns(s)

      val nproj =
        if (ns.from.length == 1)
          IdnExp(IdnUse(ns.from.head.p.get.asInstanceOf[PatternIdn].idn.idn))
        else
          RecordCons(
            ns.from.zip(s.from).zipWithIndex map {
              case ((Gen(Some(PatternIdn(IdnDef(idn))), _), Gen(Some(PatternIdn(IdnDef(origIdn))), _)), _) if !origIdn.startsWith("$") =>
                AttrCons(origIdn, IdnExp(IdnUse(idn)))
              case ((Gen(Some(PatternIdn(IdnDef(idn))), _), Gen(Some(PatternIdn(IdnDef(origIdn))), _)), idx) =>
                AttrCons(s"_${idx + 1}", IdnExp(IdnUse(idn)))})

      if (ns.where.isDefined)
        Select(ns.from, ns.distinct, None, nproj, Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), deepclone(s.group.get), ns.group.get))), None, None)
      else
        Select(ns.from, ns.distinct, None, nproj, Some(BinaryExp(Eq(), deepclone(s.group.get), ns.group.get)), None, None)
  }

  private lazy val starSelect: Select => Select = attr {
    s =>
      val ns = rewriteIdns(s)

      if (ns.where.isDefined)
        Select(ns.from, ns.distinct, None, Star(), Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), deepclone(s.group.get), ns.group.get))), None, None)
      else
        Select(ns.from, ns.distinct, None, Star(), Some(BinaryExp(Eq(), deepclone(s.group.get), ns.group.get)), None, None)
  }

  /** De-sugar a SELECT with a GROUP BY
    */

  private lazy val selectGroupBy = rule[Exp] {
    case select @ Select(from, distinct, Some(groupby), proj, where, None, None) =>
      logger.debug(s"Inside select $select")

      // Desugar * and partition from the projection side.
      val starReplacement = starSelect(select)
      val partitionReplacement = partitionSelect(select)

      val starInRest = rule[Exp] {
        case s: Star =>
          logger.debug(s"ping ping")
          deepclone(starReplacement)
      }

      val partitionInRest = rule[Exp] {
        case p: Partition =>
          logger.debug(s"ping pong")
          deepclone(partitionReplacement)
      }

      val nproj = rewrite(alltd(starInRest <+ partitionInRest))(proj)

      Select(from, distinct, None, nproj, where, None, None)
  }
}

// stop using $0 and start using s$0 ?
// humm...
// then at least i have both the name and the unique thing to replace at will, using rewrieInternalIdns
// i also have to change ALL testcases :)