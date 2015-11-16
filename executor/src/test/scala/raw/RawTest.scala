package raw

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FunSuite
import raw.calculus.{CalculusPrettyPrinter, DebugSyntaxAnalyzer}

class RawTest extends FunSuite with LazyLogging {

  /** Compare results of actual comprehension result with expected result, but first normalizes variable names.
    * Since variable names are auto-generated, the function normalizes all uses of $N to a uniform integer.
    * e.g.
    *   `$14 + $15 - 2 * $14` is equivalent to `$0 + $1 - 2 * $0`
    *   ... and ...
    *   `s$0 + s$1 - s$0` is equivalent to `s$15 + s$20 - s$15`
    */
  def compare(actual: String, expected: String): Boolean = {

    def norm(in: Iterator[String]) = {
      val m = scala.collection.mutable.Map[(String, Int), Int]()
      var cnt = 0
      for (el <- in) {
        val idn = el.takeWhile(_ != '$')
        val n = el.dropWhile(_ != '$').drop(1).toInt
        val c = m.getOrElseUpdate((idn, n), cnt)
        if (c == cnt)
          cnt += 1
      }
      m.map { case ((idn, on), nn) => s"$idn$$$on" -> s"$idn\\$$$nn" }.toMap
    }

    val r = """([a-zA-Z0-9_]*\$\d+)""".r

    def rewritten(q: String) = {
      val map1 = norm(r.findAllIn(q))
      r.replaceAllIn(q, _ match { case m => map1(m.matched) })
    }

    def fix(q: String) = {
      q.trim().replaceAll("\n+", " ").replaceAll("\\s+", " ").replaceAll("\\(\\s+", "(").replaceAll("\\s+\\)", ")").replaceAll("\\s+,\\s+", ", ")
    }

    val parsedA = actual   // CalculusPrettyPrinter(parse(actual))
    val parsedE = expected // CalculusPrettyPrinter(parse(expected))

    val A = fix(rewritten(parsedA))
    val E = fix(rewritten(parsedE))
    logger.debug(s"A $A")
    logger.debug(s"E $E")

//    val rr = DebugSyntaxAnalyzer(expected) match {
//      case Right(ast) => ast
//      case Left(err) => throw new RuntimeException(s"Expected test result could not be parsed: $err")
//    }
//    logger.debug(s"Result is parsing is ${CalculusPrettyPrinter(rr)}")

    // TODO: ok; now there's a debug parser: implement a nice compare function
    //       then can parenthesize the pretty printers
    //

    A == E
  }
}
