package raw
package calculus

import org.kiama.attribution.Attribution

class GroupByDesugarer(val analyzer: SemanticAnalyzer) extends Attribution with SemanticTransformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._
  import SymbolTable._

  def strategy = desugar

  private lazy val desugar =
    manybu(selectGroupBy)
//    manybu(
//      attempt(repeat(starInRest)) <*
//      attempt(repeat(partitionInRest)) <*
//      attempt(repeat(starInFrom)) <*
//      attempt(repeat(partitionInFrom)) <*
//      attempt(removeGroupBy))
//  manybu(
//    attempt(repeat(starInRest)) <*
//      attempt(repeat(partitionInRest)) <*
//      attempt(repeat(starInFrom)) <*
//      attempt(repeat(partitionInFrom)) <*
//      attempt(removeGroupBy))

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
      val ns = rewriteIdns(deepclone(s))

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
      val ns = rewriteIdns(deepclone(s))

      if (ns.where.isDefined)
        Select(ns.from, ns.distinct, None, Star(), Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), deepclone(s.group.get), ns.group.get))), None, None)
      else
        Select(ns.from, ns.distinct, None, Star(), Some(BinaryExp(Eq(), deepclone(s.group.get), ns.group.get)), None, None)
  }

  /** Walk up tree until we find a Select, if it exists.
    */
  private def findSelect(n: RawNode): Option[Select] = n match {
    case s: Select                            => Some(s)
    case n1 if analyzer.tree.isRoot(n1)       => None
    case analyzer.tree.parent.pair(_, parent) => findSelect(parent)
  }

  /** Returns the SELECT of a * if it exists.
    */
  private def starSelect(s: Star): Option[Select] = analyzer.starEntity(s) match {
    case StarEntity(select, _) => Some(select)
    case _ => None
  }

  /** Returns the SELECT of a partition if it exists.
    */
  private def partitionSelect(p: Partition): Option[Select] = analyzer.partitionEntity(p) match {
    case PartitionEntity(select, _) => Some(select)
    case _ => None
  }

  /** Check if *'s SELECT is the closest SELECT.
    */
  private def isClosestStar(s: Star): Boolean = (starSelect(s), findSelect(s)) match {
    case (Some(select), Some(select1)) if select eq select1 => true
    case _ => false
  }

  /** Check if partition's SELECT is the closest SELECT.
    */
  private def isClosestPartition(p: Partition): Boolean = (partitionSelect(p), findSelect(p)) match {
    case (Some(select), Some(select1)) if select eq select1 => true
    case _ => false
  }

  /** Checks whether the Select has a GROUP BY; otherwise, none of this applies.
    */
  // TODO: Without this, we're replacing WAY too much!!!

  // ok, so i replace by smtg
  // then i need to go inside?
  // no; i should be going topdown and figure it out from there
  // but then my entities fail!!!
  // well, collect them first
  //

  /** Desugar *
    */


  private lazy val starInFrom = rule[Exp] {
    case s: Star if starSelect(s).get.group.isDefined =>
      analyzer.starEntity(s) match {
        case StarEntity(select, _) => starSelect(select)
      }
  }

  /** Desugar partition
    */


  private lazy val partitionInFrom = rule[Exp] {
    case p: Partition =>
      analyzer.partitionEntity(p) match {
        case PartitionEntity(select, _) => partitionSelect(select)
      }
  }

  /** Remove GROUP BY from SELECT
    */

  private lazy val removeGroupBy = rule[Exp] {
    case s: Select if s.group.isDefined =>
      Select(s.from, s.distinct, None, s.proj, s.where, s.order, s.having)
  }


//
//  private lazy val dropGroupBy = rule[Exp] {
//    case Select(from, distinct, Some(_), proj, where, order, having) =>
//      Select(from, distinct, None, proj, where, order, having)
//  }

  /** De-sugar a SELECT with a GROUP BY
    */
//
////
//  // TODO: Rewrite this to use the partition entity notion.
//  // TODO: Could be split into 2 rules: one to replace partition by a corresponding Select,
//  // TODO: and another to rewrite the Select itself to remove the groupby part.
//  // TODO: Although removing the groupby part isn't exactly mission critical.
//  // TODO: If done bottom up, I don't see an issue, as long as the two rules are done together.
  private lazy val selectGroupBy = rule[Exp] {
    case select @ Select(from, distinct, Some(groupby), proj, where, None, None) =>
      logger.debug(s"Inside select $select")

      // Desugar * and partition from the projection side.

      val starInRest = rule[Exp] {
        case s: Star =>
          logger.debug(s"ping ping")
          deepclone(starSelect(select))
//          analyzer.starEntity(s) match {
//              // TODO: I think this check is redundant? since it must refer always to us?
//            case StarEntity(select1, _) if select eq select1 => starSelect(select)
//          }
      }

      val partitionInRest = rule[Exp] {
        case p: Partition =>
          logger.debug(s"ping pong")
          deepclone(partitionSelect(select))
//          analyzer.partitionEntity(p) match {
//            case PartitionEntity(select1, _) if select eq select1 => partitionSelect(select)
//        }
      }

      val nproj = rewrite(alltd(starInRest <+ partitionInRest))(proj)
      //

      Select(from, distinct, None, nproj, where, None, None)
//
//      logger.debug(s"Applying selectGroupBy to ${CalculusPrettyPrinter(s)}")
//      val ns = rewriteInternalIdns(deepclone(s))
//
//      assert(ns.from.nonEmpty)
//
//      val nproj =
//        if (ns.from.length == 1)
//          IdnExp(IdnUse(ns.from.head.p.get.asInstanceOf[PatternIdn].idn.idn))
//        else
//          // TODO: THis is now INCORRECT ALCTUALLY
//          RecordCons(ns.from.zipWithIndex.map { case (f, idx) => AttrCons(s"_${idx + 1}", IdnExp(IdnUse(f.p.get.asInstanceOf[PatternIdn].idn.idn)))})
//
//      val partition =
//        if (ns.where.isDefined)
//          Select(ns.from, false, None, nproj, Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), deepclone(groupby), ns.group.get))), None, None)
//        else
//          Select(ns.from, false, None, nproj, Some(BinaryExp(Eq(), deepclone(groupby), ns.group.get)), None, None)
//
//      val projWithoutPart = rewrite(everywherebu(rule[Exp] {
//        case p: Partition => // if analyzer.partitionEntity.asInstanceOf[PartitionEntity].s eq s =>
//          deepclone(partition)
//      }))(proj)
//
//      val os = Select(from, distinct, None, projWithoutPart, where, None, None)
//      logger.debug(s"Output is ${CalculusPrettyPrinter(os)}")
//      os
  }
}
