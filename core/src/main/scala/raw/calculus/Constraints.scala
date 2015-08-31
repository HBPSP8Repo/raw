package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

object Constraint extends LazyLogging {

  import raw.calculus.Calculus.Exp

  sealed abstract class Constraint extends RawNode

  case class And(cs: Constraint*) extends Constraint

  case class Or(cs: Constraint*) extends Constraint

  case class SameType(e1: Exp, e2: Exp, desc: Option[String] = None) extends Constraint

//  case class HasAttr(t: Type, a: AttrType) extends Constraint

  // TODO: IsCommutative? IsIdempotent?

  case class HasType(e: Exp, texpected: Type, desc: Option[String] = None) extends Constraint

  case object NoConstraint extends Constraint

  //    case class Collection(t: Type) extends Constraint

  import scala.language.implicitConversions

  implicit class EqConstraint(val e1: Exp) {
    def ===(e2: Exp) = SameType(e1, e2)
  }

}

//object Foo extends App {
//  println("Hello world")
//
//  val student = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Student"))
//  val students = ListType(student)
//  val professor = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Professors"))
//  val professors = ListType(professor)
//  val world = new World(sources = Map("students" -> students, "professors" -> professors))
//
//  SyntaxAnalyzer("""\(x, y) -> x.age = y""") match {
//    case Right(ast) =>
//      val t = new Calculus.Calculus(ast)
//      val analyzer = new SemanticAnalyzer(t, world)
//      analyzer.errors match {
//        case Some(err) => println("ERROR: " + err)
//        case None => println("Root type is " + analyzer.tipe2(t.root))
//      }
//  }
//
//}