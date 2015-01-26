package raw.executor.reference

import org.scalatest._
import raw._
import algebra._
import PhysicalAlgebra._

/**
 * Created by gaidioz on 1/14/15.
 */

abstract class ExecutorTest extends FeatureSpec with GivenWhenThen with  Matchers {

  val world: World

  def toString(value: Value): String = value match {
    case IntValue(c) => c.toString()
    case BooleanValue(c) => c.toString()
    case FloatValue(c) => c.toString()
    case StringValue(c) => "\"" + c.toString() + "\""
    case SetValue(s) => s.map(toString).mkString("{", ", ", "}")
    case ListValue(s) => s.map(toString).mkString("[", ", ", "]")
    case RecordValue(s) => s.map({v => v._1 + ": " + toString(v._2)}).mkString("[", ", ", "]")
  }

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

  val location = MemoryLocation(List(Map("value" -> 1)))
  val tipe = CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType()))))
  val world: World = new World(Map("oneRow" -> Source(tipe, location)))

  def checkExpression(exp: Exp, result: Any): Unit = {
    scenario("evaluation of " + exp) {
      When("evaluating " + exp)
      Then("it should return " + result)
      ReferenceExecutor.execute(Reduce(SetMonoid(), exp, List(), Select(List(), Scan(tipe, location))), world) match {
        case Right(q) => assert(q.value === Set(result))
        case _ => assert(false)
      }
    }
  }

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

  val location = MemoryLocation(List(Map("value" -> 1, "name" -> "one"), Map("value" -> 2, "name" -> "two")))
  val tipe = CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType()), AttrType("name", StringType()))))
  val world: World = new World(Map("twoRows" -> Source(tipe, location)))

  //checkOperation(Scan("oneRow"), List(RecordValue(Map("value" -> IntValue(1)))))
  //checkOperation(Reduce(ListMonoid(), Arg(0), List(), Scan(tipe, location)), List(Map("value" -> 1), Map("value" -> 2)))
  //checkOperation(Reduce(SetMonoid(), Arg(0), List(), Scan(tipe, location)), Set(Map("value" -> 1), Map("value" -> 2)))

  //checkOperation(Select(List(BoolConst(true)), Scan("oneRow")), List(RecordValue(Map("value" -> IntValue(1)))))
  checkOperation(Reduce(ListMonoid(), Arg(0), List(), Select(List(), Scan(tipe, location))), List(Map("value" -> 1, "name" -> "one"), Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), List(), Select(List(BinaryExp(Eq(),RecordProj(Arg(0),"value"), IntConst(1))), Scan(tipe, location))), List(Map("value" -> 1, "name" -> "one")))
  checkOperation(Reduce(ListMonoid(), Arg(0), List(), Select(List(BinaryExp(Eq(),RecordProj(Arg(0),"value"), IntConst(2))), Scan(tipe, location))), List(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), List(), Select(List(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("two"))), Scan(tipe, location))), List(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(ListMonoid(), Arg(0), List(), Select(List(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("three"))), Scan(tipe, location))), List())
  checkOperation(Reduce(SetMonoid(), Arg(0), List(), Select(List(BinaryExp(Eq(),RecordProj(Arg(0),"name"), StringConst("two"))), Scan(tipe, location))), Set(Map("value" -> 2, "name" -> "two")))
  checkOperation(Reduce(SetMonoid(),RecordProj(Arg(0),"name"),List(),Select(List(BinaryExp(Eq(),RecordProj(Arg(0),"value"),IntConst(1))), Scan(tipe, location))), Set("one"))
  /*
  checkOperation(Select(List(BoolConst(false)), Scan("oneRow")), List())
  checkOperation(Select(List(), Scan("oneRow")), List(RecordValue(Map("value" -> IntValue(1)))))

  checkOperation(Select(List(), Scan("threeRows")), List(RecordValue(Map("value" -> IntValue(1))), RecordValue(Map("value" -> IntValue(2))), RecordValue(Map("value" -> IntValue(3)))))
  checkOperation(Reduce(ListMonoid(), RecordProj(Arg(0), "value"), List(), Select(List(), Scan("oneRow"))), List(ListValue(List(IntValue(1)))))
  checkOperation(Reduce(ListMonoid(), RecordProj(Arg(0), "value"), List(), Select(List(), Scan("twoRows"))), List(ListValue(List(IntValue(1), IntValue(2)))))
  */
}