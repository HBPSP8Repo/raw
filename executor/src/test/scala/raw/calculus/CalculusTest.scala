package raw.calculus

import raw.RawTest

class CalculusTest extends RawTest {

  def parse(q: String): Calculus.Exp = {
    SyntaxAnalyzer(q) match {
      case Right(ast) => ast
      case Left(err) => fail(s"Parser error: $err")
    }
  }

}
