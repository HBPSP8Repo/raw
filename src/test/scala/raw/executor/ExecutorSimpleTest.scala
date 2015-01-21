package raw.executor

import raw._
import logical.Algebra._

import org.scalatest._
import raw.calculus.SymbolTable.ClassEntity
import raw.calculus.{Driver, World}

/**
 * Created by gaidioz on 1/14/15.
 */

abstract class ExecutorTest extends FeatureSpec with GivenWhenThen with  Matchers {

  val world: World
  val database: Map[String, DataLocation]

  def toString(value: MyValue): String = value match {
    case IntValue(c) => c.toString()
    case BooleanValue(c) => c.toString()
    case FloatValue(c) => c.toString()
    case StringValue(c) => "\"" + c.toString() + "\""
    case SetValue(s) => s.map(toString).mkString("{", ", ", "}")
    case ListValue(s) => s.map(toString).mkString("[", ", ", "]")
    case RecordValue(s) => s.map({v => v._1 + ": " + toString(v._2)}).mkString("[", ", ", "]")
  }

  def checkOperation(opNode: OperatorNode, result: Any): Unit = {
    val executor = new ScalaExecutor(world, database)
    scenario("evaluation of " + opNode) {
      When("evaluating " + opNode)
      Then("it should return " + result)
      assert(executor.execute(opNode).value === result)
    }
  }

}

class ExpressionsConst extends ExecutorTest {

  val world: World = new World(Map(), Set(ClassEntity("oneRow", CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType())))))))
  val database: Map[String, DataLocation] = Map("oneRow" -> MemoryLocation(List(Map("value" -> 1))))

  def checkExpression(exp: Exp, result: Any): Unit = {
    val executor = new ScalaExecutor(world, database)
    scenario("evaluation of " + exp) {
      When("evaluating " + exp)
      Then("it should return " + result)
      assert(executor.execute(Reduce(SetMonoid(), exp, List(), Select(List(), Scan("oneRow")))).value === Set(result))
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

  val world: World = new World(Map(), Set(ClassEntity("oneRow", CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType()))))), ClassEntity("twoRows", CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType())))))))
  val database: Map[String, DataLocation] = Map(
    "oneRow" -> MemoryLocation(List(Map("value" -> 1))),
    "twoRows" -> MemoryLocation(List(Map("value" -> 1), Map("value" -> 2))),
    "threeRows" -> MemoryLocation(List(Map("value" -> 1), Map("value" -> 2), Map("value" -> 3)))
  )

  //checkOperation(Scan("oneRow"), List(RecordValue(Map("value" -> IntValue(1)))))
  checkOperation(Reduce(ListMonoid(), Arg(0), List(), Scan("twoRows")), List(Map("value" -> 1), Map("value" -> 2)))
  checkOperation(Reduce(SetMonoid(), Arg(0), List(), Scan("twoRows")), Set(Map("value" -> 1), Map("value" -> 2)))

  //checkOperation(Select(List(BoolConst(true)), Scan("oneRow")), List(RecordValue(Map("value" -> IntValue(1)))))
  //checkOperation(Select(List(), Scan("twoRows")), List(RecordValue(Map("value" -> IntValue(1))), RecordValue(Map("value" -> IntValue(2)))))
  /*
  checkOperation(Select(List(BoolConst(false)), Scan("oneRow")), List())
  checkOperation(Select(List(), Scan("oneRow")), List(RecordValue(Map("value" -> IntValue(1)))))

  checkOperation(Select(List(), Scan("threeRows")), List(RecordValue(Map("value" -> IntValue(1))), RecordValue(Map("value" -> IntValue(2))), RecordValue(Map("value" -> IntValue(3)))))
  checkOperation(Reduce(ListMonoid(), RecordProj(Arg(0), "value"), List(), Select(List(), Scan("oneRow"))), List(ListValue(List(IntValue(1)))))
  checkOperation(Reduce(ListMonoid(), RecordProj(Arg(0), "value"), List(), Select(List(), Scan("twoRows"))), List(ListValue(List(IntValue(1), IntValue(2)))))
  */
}

class RealQueries extends ExecutorTest {

  val worldTypes = Map("number" -> IntType())
  val world: World = new World(worldTypes, Set(ClassEntity("oneRow", CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType()))))), ClassEntity("twoRows", CollectionType(ListMonoid(), RecordType(List(AttrType("value", IntType())))))))
  val database: Map[String, DataLocation] = Map("oneRow" -> MemoryLocation(List(Map("value" -> 1))), "twoRows" -> MemoryLocation(List(Map("value" -> 1), Map("value" -> 2))))

  def checkQuery(query: String, result: Any): Unit = {
    val opNode = world.unnest(Driver.parse(query))
    val executor = new ScalaExecutor(world, database)
    scenario("evaluation of " + query) {
      When("evaluating '" + query +"'")
      Then("it should return " + result)
      assert(executor.execute(opNode).value === result)
    }
  }
  checkQuery("for (d <- oneRow) yield set true", Set(true))
  checkQuery("for (d <- oneRow) yield list true", List(true))
  checkQuery("for (d <- twoRows) yield set true", Set(true))
  checkQuery("for (d <- twoRows) yield list true", List(true, true))
  checkQuery("for (d <- oneRow) yield max d.value", 1)
  checkQuery("for (d <- twoRows) yield max d.value", 2)
  checkQuery("for (d <- oneRow) yield list d.value", List(1))
  checkQuery("for (d <- twoRows) yield set d.value", Set(1,2))
  checkQuery("for (d <- twoRows) yield list d.value", List(1,2))

}
