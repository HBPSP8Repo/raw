package raw.calculus

import org.scalatest.FunSuite
import raw.World

class FunTest extends FunSuite {

  def parse(q: String): Calculus.Comp = {
    val parser = new SyntaxAnalyzer()
    parser.makeAST(q) match {
      case Right(ast) => ast
      case _ => ???
    }
  }
//
//    Query. (q, w) match {
//      case Right(c) =>
//    }
//  }
//    def parse(q: String, w: World): Either[QueryError, Calculus.Comp] = {
//
//
//    val c = Driver.parse(q)
//    assert(w.errors(c).length === 0)
//    c
//  }
//
//  def normalize(q: String, w: World, ast: Calculus.Comp): Either[QueryError, Calculus.Comp] = {
//    val unnester = new Unnester {
//      val userTypes = w.userTypes
//      val catalog: Set[ClassEntity] = w.catalog.map{ v => ClassEntity(v._1, v._2.tipe) }.toSet
//    }
//    val errors = unnester.errors(ast)
//    if (errors.length > 0) {
//      Left(SemanticErrors(errors))
//    } else {
//      Right(unnester.unnest(ast))
//    }
//  }




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
