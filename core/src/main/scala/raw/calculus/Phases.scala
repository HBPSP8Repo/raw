package raw
package calculus

import java.lang.reflect.Constructor

import com.typesafe.scalalogging.LazyLogging
import org.kiama.relation.Tree
import raw.calculus.Calculus.Exp

sealed abstract class Phase
case object DesugarPhase extends Phase
case object TranslatorPhase extends Phase
case object UniquifierPhase extends Phase
case object PreNormalizeSimplifierPhase extends Phase
case object NormalizerPhase extends Phase
case object PostNormalizeSimplifierPhase extends Phase
case object UnnesterPhase extends Phase
case object OptimizerPhase extends Phase

object Phases extends LazyLogging {

  import org.kiama.rewriting.Rewriter._

  // TODO: The code should be such that we could not construct objects at a given phase if certain properties that the
  //       phase requires (e.g. unique names or fresh tree) are not ensured. And this should be done at compile time!

  private val phases = Seq(
    DesugarPhase                  -> Seq( "Uniquifier1" -> (classOf[Uniquifier], true),
                                          "Desugarer"   -> (classOf[Desugarer], false)),
    TranslatorPhase               -> Seq( "Translator"  -> (classOf[Translator], false)),
    UniquifierPhase               -> Seq( "Uniquifier2" -> (classOf[Uniquifier], true)),
    PreNormalizeSimplifierPhase   -> Seq( "Simplifier1" -> (classOf[Simplifier], true)),
    NormalizerPhase               -> Seq( "Normalizer"  -> (classOf[Normalizer], false)),
    PostNormalizeSimplifierPhase  -> Seq( "Simplifier2" -> (classOf[Simplifier], true)),
    UnnesterPhase                 -> Seq( "Canonizer"   -> (classOf[Canonizer], false),
                                          "Unnester"    -> (classOf[Unnester], false)),
    OptimizerPhase                -> Seq( "Simplifier3" -> (classOf[Simplifier], true),
                                          "Optimizer"   -> (classOf[Optimizer], true))  // TODO: Ayee!
  )

  def apply(tree: Calculus.Calculus, world: World, lastPhase: Option[Phase] = None, lastTransform: Option[String] = None): Calculus.Calculus = {
    var input = tree
    for ((phase, transformers) <- phases) {
      var strategy = id
      logger.debug(s"***** BEGIN PHASE $phase *****")
      logger.debug(s"Input: ${CalculusPrettyPrinter(input.root)}")
      val semanticAnalyzer = new SemanticAnalyzer(input, world)
      for ((name, (transform, analyzer)) <- transformers) {
        // TODO: This hack forces us to use reflection on the constructors, to know which one to use!
        val t =
          if (analyzer)
            transform.getConstructor(classOf[SemanticAnalyzer]).newInstance(semanticAnalyzer)
          else
            transform.newInstance()
        strategy = attempt(strategy) <* t.strategy
        logger.debug(s"Added strategy $name")
        if (lastTransform.isDefined && name == lastTransform.get) {
          val output = rewriteTree(strategy)(input)
          logger.debug(s"Output: ${CalculusPrettyPrinter(output.root)}")
          logger.debug(s"***** END PHASE $phase ON lastTransform *****")
          return output
        }
      }
      val output = rewriteTree(strategy)(input)
      logger.debug(s"Output: ${CalculusPrettyPrinter(output.root)}")
      logger.debug(s"***** END PHASE $phase *****")
      if (lastPhase.isDefined && phase == lastPhase.get)
        return output
      input = output
    }
    input
  }

}
