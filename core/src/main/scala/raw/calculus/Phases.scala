package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

sealed abstract class Phase
case object ExpressionsDesugarPhase extends Phase
case object DesugarPhase extends Phase
case object TranslatorPhase extends Phase
case object UniquifyInputPhase extends Phase
case object PrepareForNormalizerPhase extends Phase
case object NormalizerPhase extends Phase
case object PrepareForUnnesterPhase extends Phase
case object UnnesterPhase extends Phase
case object SimplifierPhase extends Phase
case object OptimizerPhase extends Phase

object Phases extends LazyLogging {

  import org.kiama.rewriting.Rewriter._

  // TODO: The code should be such that we could not construct objects at a given phase if certain properties that the
  //       phase requires (e.g. unique names or fresh tree) are not ensured. And this should be done at compile time!

  private val phases = Seq(
    UniquifyInputPhase               -> Seq( "Uniquifier1" -> classOf[Uniquifier]),
    DesugarPhase                  -> Seq( "ExpressionsDesugarer"  -> classOf[ExpressionsDesugarer],
                                          "BlocksDesugarer"       -> classOf[BlocksDesugarer]),
    TranslatorPhase               -> Seq( "Translator"            -> classOf[Translator]),
    PrepareForNormalizerPhase             -> Seq( "Uniquifier2" -> classOf[Uniquifier]),
    NormalizerPhase               -> Seq( "Simplifier1" -> classOf[Simplifier],
                                          "Normalizer"  -> classOf[Normalizer]),
    PrepareForUnnesterPhase             -> Seq( "Uniquifier3" -> classOf[Uniquifier]),
    UnnesterPhase                 -> Seq( "Simplifier2" -> classOf[Simplifier],
                                          "Canonizer"   -> classOf[Canonizer],
                                          "Unnester"    -> classOf[Unnester]),
    SimplifierPhase               -> Seq( "Simplifier3" -> classOf[Simplifier]),
    OptimizerPhase                -> Seq( "Optimizer"   -> classOf[Optimizer])
  )

  def apply(tree: Calculus.Calculus, world: World, lastPhase: Option[Phase] = None, lastTransform: Option[String] = None): Calculus.Calculus = {
    var input = tree
    logger.debug(s"Input: ${CalculusPrettyPrinter(input.root)}")
    for ((phase, transformers) <- phases) {
      var strategy = id
      logger.debug(s"***** BEGIN PHASE $phase *****")
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
          } else
            transform.newInstance()
        strategy = attempt(strategy) <* t.strategy
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
      logger.debug(s"***** END PHASE $phase *****")
      logger.debug(s"Output: ${CalculusPrettyPrinter(output.root)}")
      if (lastPhase.isDefined && phase == lastPhase.get) {
        logger.debug(s"***** DONE DUE TO lastPhase *****")
        return output
      }
      input = output
    }
    logger.debug(s"***** DONE *****")
    input
  }

}
