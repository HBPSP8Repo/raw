package raw
package calculus

abstract class CalculusTest extends FunTest {

  /** The phase to test.
    */
  val phase: String

  /** Entry-point for most testcases.
    */
  def check(query: String, expected: String, world: World = TestWorlds.empty, ignoreRootTypeComparison: Boolean = false) =
    assert(compare(process(query, world, ignoreRootTypeComparison = ignoreRootTypeComparison), expected))

  /** Process query up until the phase.
    * Compares the input and output types.
    */
  def process(q: String, w: World = TestWorlds.empty, ignoreRootTypeComparison: Boolean = false) = {
    val t = new Calculus.Calculus(parse(q))

    val analyzer = new SemanticAnalyzer(t, w)
    logger.debug(s"Input tree:\n${CalculusPrettyPrinter(t.root, 200)}")
    if (analyzer.errors.nonEmpty) {
      logger.error("Errors found!")
      analyzer.errors.foreach(err => logger.error(ErrorsPrettyPrinter(err)))
      assert(false)
    }
    val troot = FriendlierPrettyPrinter(analyzer.tipe(t.root))
    logger.debug(s"Input root type: $troot")

    val t1 = Phases(t, w, lastTransform = Some(phase))

    val analyzer1 = new SemanticAnalyzer(t1, w)
    logger.debug(s"Output tree:\n${CalculusPrettyPrinter(t1.root, 200)}")
    if (analyzer1.errors.nonEmpty) {
      logger.error("Errors found!")
      analyzer1.errors.foreach(err => logger.error(ErrorsPrettyPrinter(err)))
      assert(false)
    }
    val troot1 = FriendlierPrettyPrinter(analyzer1.tipe(t1.root))

    logger.debug(s"Input root type: $troot")
    logger.debug(s"Output root type: $troot1")

    if (!ignoreRootTypeComparison) {
      assert(compare(troot, troot1), "Different root types found!")
    } else if (!compare(troot, troot1)) {
      logger.warn(s"Different root types found but I was asked to ignore it (likely loss of precision in cloned monoid variables?)")
    }

    CalculusPrettyPrinter(t1.root, 200)
  }


}
