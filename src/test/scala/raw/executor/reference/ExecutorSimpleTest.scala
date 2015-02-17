package raw.executor.reference

import org.scalatest._
import raw._
import algebra._
import Algebra._

/**
 * Created by gaidioz on 1/14/15.
 */

abstract class ExecutorTest extends FeatureSpec with GivenWhenThen with  Matchers {

  // default very basic content for our database
  val location = MemoryLocation(List(Map("value" -> 1)))
  val tipe = ListType(RecordType(List(AttrType("value", NumberType()))))
  val world: World = new World(Map("oneRow" -> Source(tipe, location)))

  // asserts that an expression is properly evaluated to a certain result
  def checkExpression(exp: Exp, result: Any): Unit = {
    scenario("evaluation of " + exp) {
      When("evaluating " + exp)
      Then("it should return " + result)
      ReferenceExecutor.execute(Reduce(SetMonoid(), exp, BoolConst(true), Select(BoolConst(true), Scan("oneRow"))), world) match {
        case Right(q) => assert(q.value === Set(result))
        case _ => assert(false)
      }
    }
  }

  // asserts an operation returns the expected result
  def checkOperation(opNode: OperatorNode, result: Any): Unit = {
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

  checkExpression(NumberConst("1"), 1)
  checkExpression(NumberConst("2"), 2)
  checkExpression(BoolConst(true), true)
  checkExpression(BoolConst(false), false)
  checkExpression(NumberConst("22.23f"), 22.23f)
  checkExpression(NumberConst("3.14f"), 3.14f)
  checkExpression(StringConst("tralala"), "tralala")

  def checkUnaryExp(exp: Exp, results: Map[UnaryOperator, Any]): Unit = {
    val f = (op: UnaryOperator, result: Any) => checkExpression(UnaryExp(op, exp), result)
    results.foreach { case (op, result) => f(op, result)}
  }

  checkUnaryExp(BoolConst(true), Map(Not() -> false, ToBool() -> true))
  checkUnaryExp(BoolConst(false), Map(Not() -> true, ToBool() -> false))
  checkUnaryExp(NumberConst("1"), Map(Not() -> false, Neg() -> -1, ToBool() -> true))
  checkUnaryExp(NumberConst("0"), Map(Not() -> true, Neg() -> 0, ToBool() -> false))
  checkUnaryExp(NumberConst("-1"), Map(Not() -> false, Neg() -> 1, ToBool() -> true))
  checkUnaryExp(NumberConst("1.1f"), Map(Not() -> false, Neg() -> -1.1f, ToBool() -> true))
  checkUnaryExp(NumberConst("0.0f"), Map(Not() -> true, Neg() -> 0.0f, ToBool() -> false))
  checkUnaryExp(NumberConst("-1.1f"), Map(Not() -> false, Neg() -> 1.1f, ToBool() -> true))

  def checkBinExp(exp1: Exp, exp2: Exp, results: Map[BinaryOperator, Any]): Unit = {
    val f = (op: BinaryOperator, result: Any) => checkExpression(BinaryExp(op, exp1, exp2), result)
    results.foreach { case (op, result) => f(op, result) }
  }

  checkBinExp(NumberConst("1"), NumberConst("1"), Map(Eq() -> true, Neq() -> false))
  checkBinExp(NumberConst("1"), NumberConst("2"), Map(Eq() -> false, Neq() -> true))
  checkBinExp(NumberConst("1.2f"), NumberConst("1.2f"), Map(Eq() -> true, Neq() -> false))
  checkBinExp(NumberConst("1.1f"), NumberConst("1.2f"), Map(Eq() -> false, Neq() -> true))
  checkBinExp(BoolConst(true), BoolConst(true), Map(Eq() -> true, Neq() -> false))
  checkBinExp(BoolConst(true), BoolConst(false), Map(Eq() -> false, Neq() -> true))
  checkBinExp(StringConst("tralala"), StringConst("tralala"), Map(Eq() -> true, Neq() -> false))
  checkBinExp(StringConst("tralala"), StringConst("tralalere"), Map(Eq() -> false, Neq() -> true))
  
  checkExpression(ZeroCollectionMonoid(SetMonoid()), Set())
  checkExpression(ZeroCollectionMonoid(ListMonoid()), List())
  checkExpression(ConsCollectionMonoid(SetMonoid(), NumberConst("22")), Set(22))
  checkExpression(ConsCollectionMonoid(ListMonoid(), NumberConst("22")), List(22))

  checkExpression(RecordCons(Seq(AttrCons("a", NumberConst("2")))), Map("a" -> 2))
  checkExpression(RecordCons(Seq(AttrCons("a", NumberConst("2")), AttrCons("b", NumberConst("3")))), Map("a" -> 2, "b" -> 3))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", NumberConst("2")))), "a"), 2)
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", NumberConst("2")), AttrCons("b", NumberConst("3")))), "a"), 2)
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", NumberConst("2")), AttrCons("b", NumberConst("3")))), "b"), 3)


}

class ReduceOperations extends  ExecutorTest {

  override val location = MemoryLocation(List(Map("value" -> 1, "name" -> "one"), Map("value" -> 2, "name" -> "two")))
  override val tipe = ListType(RecordType(List(AttrType("value", NumberType()), AttrType("name", StringType()))))
  override val world: World = new World(Map("twoRows" -> Source(tipe, location)))

  checkOperation(Reduce(ListMonoid(), Arg(0), BoolConst(true), Select(BoolConst(true), Scan("twoRows"))), List(Map("value" -> 1, "name" -> "one"), Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), BoolConst(true), Select(BinaryExp(Eq(),RecordProj(Arg(0),"value"), NumberConst("1")), Scan("twoRows"))), List(Map("value" -> 1, "name" -> "one")))
  checkOperation(Reduce(ListMonoid(), Arg(0), BoolConst(true), Select(BinaryExp(Eq(),RecordProj(Arg(0),"value"), NumberConst("2")), Scan("twoRows"))), List(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), BoolConst(true), Select(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("two")), Scan("twoRows"))), List(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), BoolConst(true), Select(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("three")), Scan("twoRows"))), List())
  checkOperation(Reduce(SetMonoid(), Arg(0), BoolConst(true), Select(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("two")), Scan("twoRows"))), Set(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(SetMonoid(),RecordProj(Arg(0),"name"), BoolConst(true), Select(BinaryExp(Eq(),RecordProj(Arg(0),"value"),NumberConst("1")), Scan("twoRows"))), Set("one"))
}

class JoinOperations extends ExecutorTest {

  // two tables, students (name + department) and departments (name + discipline)
  val students = MemoryLocation(List(Map("name" -> "s1", "department" -> "dep1"), Map("name" -> "s2", "department" -> "dep2"), Map("name" -> "s3", "department" -> "dep2")))
  val departments = MemoryLocation(List(Map("name" -> "dep1", "discipline" -> "Artificial Intelligence"), Map("name" -> "dep2", "discipline" -> "Operating Systems"), Map("name" -> "dep3", "discipline" -> "Robotics")))
  val studentType = ListType(RecordType(List(AttrType("name", StringType()), AttrType("department", StringType()))))
  val depType = ListType(RecordType(List(AttrType("name", StringType()), AttrType("discipline", StringType()))))
  override val world: World = new World(Map("students" -> Source(studentType, students), "departments" -> Source(depType, departments)))

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
                        Nest(SumMonoid(), NumberConst("1"), ProductCons(Seq(Arg(0))), BoolConst(true), ProductCons(Seq(Arg(1))),
                             Join(BinaryExp(Eq(), RecordProj(Arg(0), "name"), RecordProj(Arg(1), "department")),
                                  Scan("departments"), Scan("students")))),
                 Set(Map("name" -> "dep1", "count" -> 1), Map("name" -> "dep2", "count" -> 2)))

  // set of students per department (using outer join, should return dep3 with zero students)
  checkOperation(Reduce(SetMonoid(), RecordCons(List(AttrCons("name", RecordProj(Arg(0), "name")), AttrCons("count", Arg(1)))),
                        BoolConst(true),
                        Nest(SumMonoid(), NumberConst("1"), ProductCons(Seq(Arg(0))), BoolConst(true), ProductCons(Seq(Arg(1))),
                             OuterJoin(BinaryExp(Eq(), RecordProj(Arg(0), "name"), RecordProj(Arg(1), "department")),
                                       Scan("departments"), Scan("students")))),
                 Set(Map("name" -> "dep1", "count" -> 1), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 0)))
}