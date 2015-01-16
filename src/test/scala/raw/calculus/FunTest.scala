package raw.calculus

import org.scalatest.FunSuite

class FunTest extends FunSuite {

  def parse(w: World, q: String) = {
    val c = Driver.parse(q)
    assert(w.errors(c).length === 0)
    c
  }

  /** Compare results of actual comprehension result with expected result, but first normalizes variable names.
    * Since variable names are auto-generated, the function normalizes all uses of $varN to a uniform integer.
    * e.g.
    *   `$var14 + $var15 - 2 * $var14` is equivalent to `$var0 + $var1 - 2 * $var0`
    */
  def compare(actual: String, expected: String) = {
    def norm(in: Iterator[String]) = {
      var m = scala.collection.mutable.Map[String, Int]()
      var cnt = 0
      for (v <- in) {
        val c = m.getOrElseUpdate(v, cnt)
        if (c == cnt)
          cnt += 1
      }
      m
    }

    val r = """\$var\d+""".r

    def rewritten(q: String) = {
      val map = norm(r.findAllIn(q))
      r.replaceAllIn(q, _ match { case m => s"\\$$var${map(m.matched)}" })
    }

    assert(rewritten(actual) === rewritten(expected))
  }
}
