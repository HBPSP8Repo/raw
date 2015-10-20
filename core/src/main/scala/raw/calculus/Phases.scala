package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

object Phases extends LazyLogging {

  import org.kiama.rewriting.Rewriter._

  // TODO: Make use of notion of SemanticTransformer vs PipelinedTransformer to strengthen building of phases

  private val phases = Seq(
    Seq(  "AnonGensDesugarer"         -> classOf[AnonGensDesugarer]),
    Seq(  "Uniquifier1"               -> classOf[Uniquifier]),
    Seq(  "GroupByDesugarer"          -> classOf[GroupByDesugarer]),
    Seq(  "StarDesugarer"             -> classOf[StarDesugarer]),
    Seq(  "SelectDesugarer"           -> classOf[SelectDesugarer]),
    Seq(  "ExpressionsDesugarer"      -> classOf[ExpressionsDesugarer],
          "BlocksDesugarer"           -> classOf[BlocksDesugarer]),
    Seq(  "MonoidVariablesDesugarer"  -> classOf[MonoidVariablesDesugarer]),
    Seq(  "Uniquifier2"               -> classOf[Uniquifier]),
    Seq(  "Simplifier1"               -> classOf[Simplifier],
          "Normalizer"                -> classOf[Normalizer]),
    Seq(  "Simplifier2"               -> classOf[Simplifier],
          "Canonizer"                 -> classOf[Canonizer],
          "Unnester"                  -> classOf[Unnester]),
    Seq(  "Simplifier3"               -> classOf[Simplifier]),
    Seq(  "Optimizer"                 -> classOf[Optimizer])
  )

  private def transformExists(name: Option[String]): Boolean =
    if (name.isEmpty)
      true
    else {
      for (ts <- phases) {
        for ((tname, _) <- ts) {
          if (name.get == tname)
            return true
        }
      }
      false
    }

  def apply(tree: Calculus.Calculus, world: World, lastTransform: Option[String] = None): Calculus.Calculus = {
    assert(lastTransform.isEmpty || transformExists(lastTransform))

    SymbolTable.reset()

    var input = tree
    logger.debug(s"Input: ${CalculusPrettyPrinter(input.root)}")
    for (transformers <- phases) {
      var strategy = id
      logger.debug(s"***** BEGIN PHASE *****")
      val semanticAnalyzer = new SemanticAnalyzer(input, world)
      semanticAnalyzer.tipe(input.root)   // Typing the root is *required*, since some of the rules will be asking
                                          // the types of inner nodes, and those are only fully typed if the whole
                                          // program has been typed.
      var firstInPhase = true // The first transformation in a Phase needs the SemanticAnalyzer.
                              // (That's why it is a separate phase!)
      for ((name, transform) <- transformers) {
        val t =
          if (firstInPhase) {
            firstInPhase = false
            transform.getConstructor(classOf[SemanticAnalyzer]).newInstance(semanticAnalyzer)
          } else {
            transform.newInstance()
          }
        strategy = attempt(strategy) <* t.transform
        logger.debug(s"Adding rewrite strategy $name")
        if (lastTransform.isDefined && name == lastTransform.get) {
          logger.debug(s"Begin rewriting tree...")
          val output = rewriteTree(strategy)(input)
          logger.debug(s"... done rewriting tree.")
          logger.debug(s"***** DONE DUE TO lastTransform *****")
          logger.debug(s"Output: ${CalculusPrettyPrinter(output.root)}")
          return output
        }
      }
      logger.debug(s"Begin rewriting tree...")
      val output = rewriteTree(strategy)(input)
      logger.debug(s"... done rewriting tree.")
      logger.debug(s"***** END PHASE *****")
      logger.debug(s"Output: ${CalculusPrettyPrinter(output.root)}")
      input = output
    }
    logger.debug(s"***** DONE *****")
    input
  }

}
