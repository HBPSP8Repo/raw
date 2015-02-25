package raw.calculus

import org.scalatest.FunSuite
import com.typesafe.scalalogging.LazyLogging

class FunTest extends FunSuite with LazyLogging {

  def parse(q: String): Calculus.Exp = {
    val parser = new SyntaxAnalyzer()
    parser.makeAST(q) match {
      case Right(ast) => ast
      case Left(err) => println(s"Parser error: $err"); ???
    }
  }

  /** Compare results of actual comprehension result with expected result, but first normalizes variable names.
    * Since variable names are auto-generated, the function normalizes all uses of $N to a uniform integer.
    * e.g.
    *   `$14 + $15 - 2 * $14` is equivalent to `$0 + $1 - 2 * $0`
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

    val r = """\$\d+""".r

    def rewritten(q: String) = {
      val map = norm(r.findAllIn(q))
      r.replaceAllIn(q, _ match { case m => s"\\$$${map(m.matched)}" })
    }

    assert(rewritten(actual) === rewritten(expected))
  }
}
