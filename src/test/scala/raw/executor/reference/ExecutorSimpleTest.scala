package raw.executor.reference

import org.scalatest._
import raw._
import algebra._
import Algebra._
import raw.algebra.Expressions._

/**
 * Created by gaidioz on 1/14/15.
 */

abstract class ExecutorTest extends FeatureSpec with GivenWhenThen with  Matchers {

  // default very basic content for our database
  val location = MemoryLocation(List(Map("value" -> 1)))
  val tipe = CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType()))))
  val world: World = new World(Map("oneRow" -> Source(tipe, location)))

  // asserts that an expression is properly evaluated to a certain result
  def checkExpression(exp: Exp, result: Any): Unit = {
    scenario("evaluation of " + exp) {
      When("evaluating " + exp)
      Then("it should return " + result)
      ReferenceExecutor.execute(Reduce(SetMonoid(), exp, ProductCons(Seq()), Select(BoolConst(true), Scan("oneRow"))), world) match {
        case Right(q) => assert(q.value === Set(result))
        case _ => assert(false)
      }
    }
  }

  // asserts an operation returns the expected result
  def checkOperation(opNode: AlgebraNode, result: Any): Unit = {
    scenario("evaluation of " + opNode) {
      When("evaluating " + opNode)
      Then("it should return " + result)
      ReferenceExecutor.execute(opNode, world) match {
        case Right(q) => assert(q.value === result)
        case _ => assert(false)
      }
    }
  }

}

class ExpressionsConst extends ExecutorTest {

  checkExpression(IntConst(1), 1)
  checkExpression(IntConst(2), 2)
  checkExpression(BoolConst(true), true)
  checkExpression(BoolConst(false), false)
  checkExpression(FloatConst(22.23f), 22.23f)
  checkExpression(FloatConst(3.14f), 3.14f)
  checkExpression(StringConst("tralala"), "tralala")

  def checkUnaryExp(exp: Exp, results: Map[UnaryOperator, Any]): Unit = {
    val f = (op: UnaryOperator, result: Any) => checkExpression(UnaryExp(op, exp), result)
    results.foreach { case (op, result) => f(op, result)}
  }

  checkUnaryExp(BoolConst(true), Map(Not() -> false, ToInt() -> 1, ToBool() -> true, ToFloat() -> 1.0f))
  checkUnaryExp(BoolConst(false), Map(Not() -> true, ToInt() -> 0, ToBool() -> false, ToFloat() -> 0.0f))
  checkUnaryExp(IntConst(1), Map(Not() -> false, Neg() -> -1, ToInt() -> 1, ToBool() -> true, ToFloat() -> 1.0f))
  checkUnaryExp(IntConst(0), Map(Not() -> true, Neg() -> 0, ToInt() -> 0, ToBool() -> false, ToFloat() -> 0.0f))
  checkUnaryExp(IntConst(-1), Map(Not() -> false, Neg() -> 1, ToInt() -> -1, ToBool() -> true, ToFloat() -> -1.0f))
  checkUnaryExp(FloatConst(1.1f), Map(Not() -> false, Neg() -> -1.1f, ToInt() -> 1, ToBool() -> true, ToFloat() -> 1.1f))
  checkUnaryExp(FloatConst(0.0f), Map(Not() -> true, Neg() -> 0.0f, ToInt() -> 0, ToBool() -> false, ToFloat() -> 0.0f))
  checkUnaryExp(FloatConst(-1.1f), Map(Not() -> false, Neg() -> 1.1f, ToInt() -> -1, ToBool() -> true, ToFloat() -> -1.1f))

  def checkBinExp(exp1: Exp, exp2: Exp, results: Map[BinaryOperator, Any]): Unit = {
    val f = (op: BinaryOperator, result: Any) => checkExpression(BinaryExp(op, exp1, exp2), result)
    results.foreach { case (op, result) => f(op, result) }
  }

  checkBinExp(IntConst(1), IntConst(1), Map(Eq() -> true, Neq() -> false))
  checkBinExp(IntConst(1), IntConst(2), Map(Eq() -> false, Neq() -> true))
  checkBinExp(FloatConst(1.2f), FloatConst(1.2f), Map(Eq() -> true, Neq() -> false))
  checkBinExp(FloatConst(1.1f), FloatConst(1.2f), Map(Eq() -> false, Neq() -> true))
  checkBinExp(BoolConst(true), BoolConst(true), Map(Eq() -> true, Neq() -> false))
  checkBinExp(BoolConst(true), BoolConst(false), Map(Eq() -> false, Neq() -> true))
  checkBinExp(StringConst("tralala"), StringConst("tralala"), Map(Eq() -> true, Neq() -> false))
  checkBinExp(StringConst("tralala"), StringConst("tralalere"), Map(Eq() -> false, Neq() -> true))
  
  checkExpression(ZeroCollectionMonoid(SetMonoid()), Set())
  checkExpression(ZeroCollectionMonoid(ListMonoid()), List())
  checkExpression(ConsCollectionMonoid(SetMonoid(), IntConst(22)), Set(22))
  checkExpression(ConsCollectionMonoid(ListMonoid(), IntConst(22)), List(22))

  checkExpression(RecordCons(Seq(AttrCons("a", IntConst(2)))), Map("a" -> 2))
  checkExpression(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), Map("a" -> 2, "b" -> 3))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)))), "a"), 2)
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "a"), 2)
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "b"), 3)


}

class ReduceOperations extends  ExecutorTest {

  override val location = MemoryLocation(List(Map("value" -> 1, "name" -> "one"), Map("value" -> 2, "name" -> "two")))
  override val tipe = CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType()), AttrType("name", StringType()))))
  override val world: World = new World(Map("twoRows" -> Source(tipe, location)))

  checkOperation(Reduce(ListMonoid(), Arg(0), ProductCons(Seq()), Select(BoolConst(true), Scan("oneRow"))), List(Map("value" -> 1, "name" -> "one"), Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), ProductCons(Seq()), Select(BinaryExp(Eq(),RecordProj(Arg(0),"value"), IntConst(1)), Scan("oneRow"))), List(Map("value" -> 1, "name" -> "one")))
  checkOperation(Reduce(ListMonoid(), Arg(0), ProductCons(Seq()), Select(BinaryExp(Eq(),RecordProj(Arg(0),"value"), IntConst(2)), Scan("oneRow"))), List(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), ProductCons(Seq()), Select(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("two")), Scan("oneRow"))), List(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), ProductCons(Seq()), Select(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("three")), Scan("oneRow"))), List())
  checkOperation(Reduce(SetMonoid(), Arg(0), ProductCons(Seq()), Select(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("two")), Scan("oneRow"))), Set(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(SetMonoid(),RecordProj(Arg(0),"name"),ProductCons(Seq()),Select(BinaryExp(Eq(),RecordProj(Arg(0),"value"),IntConst(1)), Scan("oneRow"))), Set("one"))
}

class JoinOperations extends ExecutorTest {

  // two tables, students (name + department) and departments (name + discipline)
  val students = MemoryLocation(List(Map("name" -> "s1", "department" -> "dep1"), Map("name" -> "s2", "department" -> "dep2"), Map("name" -> "s3", "department" -> "dep2")))
  val departments = MemoryLocation(List(Map("name" -> "dep1", "discipline" -> "Artificial Intelligence"), Map("name" -> "dep2", "discipline" -> "Operating Systems"), Map("name" -> "dep3", "discipline" -> "Robotics")))
  val studentType = CollectionType(ListMonoid(), RecordType(List(AttrType("name", StringType()), AttrType("department", StringType()))))
  val depType = CollectionType(ListMonoid(), RecordType(List(AttrType("name", StringType()), AttrType("discipline", StringType()))))
  override val world: World = new World(Map("students" -> Source(studentType, students), "numbers" -> Source(studentType, students)))

  // list of (name, discipline) for all students (join with department)
  checkOperation(Reduce(ListMonoid(), RecordCons(List(AttrCons("student", RecordProj(Arg(0), "name")), AttrCons("discipline", RecordProj(Arg(1), "discipline")))),
                        BoolConst(true),
                        Join(BinaryExp(Eq(), RecordProj(Arg(0), "department"), RecordProj(Arg(1), "name")),
                             Scan("students"), Scan("departments"))),
                 List(Map("student" -> "s1", "discipline" -> "Artificial Intelligence"), Map("student" -> "s2", "discipline" -> "Operating Systems"), Map("student" -> "s3", "discipline" -> "Operating Systems")))

  // set of (student name, discipline) only if department is dep2 (join + filter during join).
  checkOperation(Reduce(SetMonoid(), RecordCons(List(AttrCons("student", RecordProj(Arg(0), "name")), AttrCons("discipline", RecordProj(Arg(1), "discipline")))),
                        BoolConst(true),
                        Join(MergeMonoid(AndMonoid(), BinaryExp(Eq(), RecordProj(Arg(0), "department"), RecordProj(Arg(1), "name")), BinaryExp(Eq(), RecordProj(Arg(1), "name"), StringConst("dep2"))),
                             Scan("students"), Scan("departments"))),
                 Set(Map("student" -> "s2", "discipline" -> "Operating Systems"), Map("student" -> "s3", "discipline" -> "Operating Systems")))

  // number of students per department (mistakenly using join: it will not show dep3 since it doesn't have students)
  checkOperation(Reduce(SetMonoid(), RecordCons(List(AttrCons("name", RecordProj(Arg(0), "name")), AttrCons("count", Arg(1)))),
                        BoolConst(true),
                        Nest(SumMonoid(), IntConst(1), ProductCons(Seq(Arg(0))), ProductCons(Seq(Arg(1))), ProductCons(Seq()),
                             Join(BinaryExp(Eq(), RecordProj(Arg(0), "name"), RecordProj(Arg(1), "department")),
                                  Scan("departments"), Scan("students")))),
                 Set(Map("name" -> "dep1", "count" -> 1), Map("name" -> "dep2", "count" -> 2)))

  // set of students per department (using outer join, should return dep3 with zero students)
  checkOperation(Reduce(SetMonoid(), RecordCons(List(AttrCons("name", RecordProj(Arg(0), "name")), AttrCons("count", Arg(1)))),
                        BoolConst(true),
                        Nest(SumMonoid(), IntConst(1), ProductCons(Seq(Arg(0))), ProductCons(Seq()), ProductCons(Seq(Arg(1))),
                             OuterJoin(BinaryExp(Eq(), RecordProj(Arg(0), "name"), RecordProj(Arg(1), "department")),
                                       Scan("departments"), Scan("students")))),
                 Set(Map("name" -> "dep1", "count" -> 1), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 0)))
}