package raw
package calculus

class CoreTest extends RawTest {

  def parse(q: String): Calculus.Exp = {
    SyntaxAnalyzer(q) match {
      case Right(ast) => ast
      case Left(err) => fail(s"Parser error: $err")
    }
  }

}
