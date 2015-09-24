package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

/** Desugar SQL to for comprehension
  */
trait Translator extends Transformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = translate

  def analyzer: SemanticAnalyzer

//  private def translate = attempt(manytd(selectGroupBy)) <* reduce(selectToComp)
    private def translate = reduce(selectGroupBy)

  // TODO: Keyword "partition" ? Type it...
  // TODO: Add case classes ToSet, ToBag, ToList and apply them here so that the output Comp still types properly

  private lazy val selectToComp = rule[Exp] {
    case s@Select(from, _, None, proj, where, None, None) =>
      val whereq = if (where.isDefined) Seq(where.head) else Seq()
      // TODO: Handlie distinct!!
      // TODO: Turn it allinto ToBag always and have the Bag Monoid unless it is distinct
      Comp(BagMonoid(), from.map { case Iterator(Some(p), e) => Gen(p, e) } ++ whereq, proj)
//      analyzer.tipe(s) match {
//        case CollectionType(m, _) => Comp(m, from.map { case Iterator(Some(p), e) => Gen(p, e) } ++ whereq, proj)
//      }
  }
//    case s @ Select(from, _, Some(g), proj, where, None, None) =>
//      val whereq = if (where.isDefined) Seq(where.head) else Seq()
//      analyzer.tipe(s) match {
//        case CollectionType(m, _) =>
//          val qs = from.map { case Iterator(Some(p), e) => Gen(p, e)} ++ whereq
//
//
//          Comp(m,
//            qs,
//            ExpBlock(
//              Seq(
//                Bind(
//                  PatternIdn(IdnDef("partition"))),
//                  Comp(deepclone(m), deepclone(qs) ++ Seq(BinaryExp(Eq, , ???), ))
//
//            )),
//            proj)
//
//
//          Comp(m, from.map { case Iterator(Some(p), e) => Gen(p, e)} ++ whereq, proj)
//      }
//
  private lazy val selectGroupBy = rule[Exp] {
    case s @ Select(from, distinct, Some(groupby), proj, where, None, None) =>
      logger.debug(s"Applying selectGroupBy")
      val ns = rewriteIdns(deepclone(s))

      val nproj =
        if (ns.from.length == 1)
          IdnExp(IdnUse(ns.from.head.idn.get.idn.idn))
        else if (ns.from.length > 1)
          RecordCons(ns.from.zipWithIndex.map { case (f, idx) => AttrCons(s"_${idx + 1}", IdnExp(IdnUse(f.idn.get.idn.idn)))})
        else
          ???

      val partition = if (ns.where.isDefined)
        Select(ns.from, ns.distinct, None, nproj, Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), groupby, ns.group.get))), None, None)
      else
        Select(ns.from, ns.distinct, None, nproj, Some(BinaryExp(Eq(), groupby, ns.group.get)), None, None)

      // TODO: Replace `s` by reference equality check
      logger.debug(s"will crash: ${CalculusPrettyPrinter(s)}")
      val projWithoutPart = rewrite(everywhere(rule[Exp] {
        case p: Partition if analyzer.partitionSelect(p).contains(`s`) =>
          deepclone(partition)
      }))(proj)

      val r = Select(from, distinct, None, projWithoutPart, where, None, None)
    logger.debug(s"Result is ${CalculusPrettyPrinter(r)}")
    r
  }



//  """"select s.age/10, partition from students s group by s.age/10""""
//  """"select s2.age/10, partition from students s2 group by s2.age/10""""
//  """"select s2 from students s2 group by s2.age/10""""
//  """"select s2 from students s2 where s2.age/10 = [ group by of top-most ]""""
//rewrite partition to be that thing.

  //  """"select s.age/10, (select s2 from students s2 where s2.age/10=s.age/10) from students s""""


  //  """for ($0 <- students) yield bag { partition := for ($1 <- students; $1.age/10 = $0.age/10) yield bag $1 ; ($0.age/10, partition) }""")


}

object Translator extends LazyLogging {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree, world)
    val a = new SemanticAnalyzer(t1, world)
    val translator = new Translator {
      override def analyzer: SemanticAnalyzer = a
    }
    logger.debug(s"T1HERE IS ${(t1.root)}")
    rewriteTree(translator.strategy)(t1)
  }
}
