package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

sealed abstract class Phase
case object DesugarPhase extends Phase
case object UniquifierPhase extends Phase
case object NormalizerPhase extends Phase
case object UnnesterPhase extends Phase
case object SimplifierPhase extends Phase
case object OptimizerPhase extends Phase

object Phases extends LazyLogging {

  import org.kiama.rewriting.Rewriter._

  // TODO: The code should be such that we could not construct objects at a given phase if certain properties that the
  //       phase requires (e.g. unique names or fresh tree) are not ensured. And this should be done at compile time!

  private val phases = Seq(
    DesugarPhase                  -> Seq( "Uniquifier1" -> classOf[Uniquifier],
                                          "Desugarer"   -> classOf[Desugarer],
                                          "Translator"  -> classOf[Translator]),
    UniquifierPhase               -> Seq( "Uniquifier2" -> classOf[Uniquifier]),
    NormalizerPhase               -> Seq( "Simplifier1" -> classOf[Simplifier],
                                          "Normalizer"  -> classOf[Normalizer]),
    UnnesterPhase                 -> Seq( "Simplifier2" -> classOf[Simplifier],
                                          "Canonizer"   -> classOf[Canonizer],
                                          "Unnester"    -> classOf[Unnester]),
    SimplifierPhase               -> Seq( "Simplifier3" -> classOf[Simplifier]),
    OptimizerPhase                -> Seq( "Optimizer"   -> classOf[Optimizer])
  )

  def apply(tree: Calculus.Calculus, world: World, lastPhase: Option[Phase] = None, lastTransform: Option[String] = None): Calculus.Calculus = {
    var input = tree
    for ((phase, transformers) <- phases) {
      var strategy = id
      logger.debug(s"***** BEGIN PHASE $phase *****")
      logger.debug(s"Input: ${CalculusPrettyPrinter(input.root)}")
      logger.debug(s"Input: ${CalculusPrettyPrinter(input.root)}")
      val semanticAnalyzer = new SemanticAnalyzer(input, world)
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
        logger.debug(s"Added strategy $name")
        if (lastTransform.isDefined && name == lastTransform.get) {
          val output = rewriteTree(strategy)(input)
          logger.debug(s"Output: ${CalculusPrettyPrinter(output.root)}")
          logger.debug(s"***** DONE DUE TO lastTransform *****")
          return output
        }
      }
      val output = rewriteTree(strategy)(input)
      logger.debug(s"Output: ${CalculusPrettyPrinter(output.root)}")
      logger.debug(s"***** END PHASE $phase *****")
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
