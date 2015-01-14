package raw.calculus

import raw.RawException

case class ParserError(err: String) extends RawException

object Driver {

  import SymbolTable.ClassEntity

  val parser = new SyntaxAnalyzer()
  
  def parse(in: String) = {
    parser.makeAST(in) match {
      case Right(ast) => ast
      case Left(e) => println(e); throw ParserError(e)
    }
  }
  
  def query(in: String, userTypes: Map[String, Type], catalog: Set[ClassEntity]) = {
    val ast = parse(in)
    val world = new World(userTypes, catalog)
    world.process(ast)
  }
  
}