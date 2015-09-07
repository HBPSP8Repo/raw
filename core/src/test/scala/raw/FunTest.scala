package raw

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FunSuite
import raw.calculus.{Calculus, SyntaxAnalyzer}

import scala.collection.mutable

class FunTest extends FunSuite with LazyLogging {

  def parse(q: String): Calculus.Exp = {
    SyntaxAnalyzer(q) match {
      case Right(ast) => ast
      case Left(err) => fail(s"Parser error: $err")
    }
  }

  /** Compare results of actual comprehension result with expected result, but first normalizes variable names.
    * Since variable names are auto-generated, the function normalizes all uses of $N to a uniform integer.
    * e.g.
    *   `$14 + $15 - 2 * $14` is equivalent to `$0 + $1 - 2 * $0`
    */
  def compare(actual: String, expected: String) = {
    def norm(in: Iterator[String]) = {
      val m = scala.collection.mutable.Map[String, Int]()
      var cnt = 0
      for (v <- in) {
        val c = m.getOrElseUpdate(v, cnt)
        if (c == cnt)
          cnt += 1
      }
      m
    }

    val r = """\$\d+""".r

    def rewritten(q: String) = {
      val map = norm(r.findAllIn(q))
      r.replaceAllIn(q, _ match { case m => s"\\$$${map(m.matched)}" })
    }

    logger.debug(s"Actual ${rewritten(actual)}")
    logger.debug(s"Exp    ${rewritten(expected)}")

    if (rewritten(actual) != rewritten(expected))
      assert(false, s"Incompatible!!\nActual: $actual\nExpected: $expected")
  }
}
