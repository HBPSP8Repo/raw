package raw

import org.kiama.util.Message
import raw.algebra.{Unnester, Algebra}
import calculus._
import raw.executor.{PhysicalPlanRewriter, Executor}
import raw.executor.reference.{ReferenceResult, ReferenceExecutor}


object PhysicalPlan {

  def parse(query: String, world: World): Either[QueryError, Calculus.Calculus] = {
    val parser = new SyntaxAnalyzer()
    parser.makeAST(query) match {
      case Right(ast) => Right(new Calculus.Calculus(ast))
      case Left(error) => Left(ParserError(error))
    }
  }

  def analyze(t: Calculus.Calculus, w: World): Option[QueryError] = {
    val analyzer = new SemanticAnalyzer(t, w)
    if (analyzer.errors.length == 0)
      None
    else
      Some(SemanticErrors(analyzer.errors))
  }

  def unnest(tree: Calculus.Calculus, world: World): Algebra.OperatorNode = Unnester(tree, world)

  def apply(query: String, world: World, executor: Executor = ReferenceExecutor): Either[QueryError, QueryResult] = {
    parse(query, world) match {
      case Right(tree) => analyze(tree, world) match {
        case None            => {
          val t : Algebra.OperatorNode = unnest(tree, world)
          executor.execute(t, world)

          val physical = new PhysicalPlanRewriter(t , world)

          physical.registerSources(t)
          val sources = physical.sourceNames(t)
          println("[Sources: ] "+sources)

//        Debugging
//          t match {
//            case r @ Algebra.Reduce( _,_,_,child) => {
//              val fields = physical.neededFields(r)(Map())
//              println("[Fields: ] "+fields)
//            }
//            case _ => println("Undefined Behavior")
//          }
          val fields = physical.neededFields(t)(Map())
          println("[Fields: ] "+fields)

          Right(new ReferenceResult(1))
        }
        case Some(err)       => Left(err)
      }
      case Left(err)       => Left(err)
    }
  }

}
