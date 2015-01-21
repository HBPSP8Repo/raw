package raw.executor

import raw._
import logical.Algebra._

import org.scalatest.{FeatureSpec, GivenWhenThen, FunSpec, FunSuite}
import raw.calculus.SymbolTable.ClassEntity
import raw.calculus.{Driver, World}

/**
 * Created by gaidioz on 1/14/15.
 */

abstract class ExecutorTest extends FeatureSpec with GivenWhenThen {

  val database: Map[String, ListValue]

  def checkExpression(exp: Exp, result: MyValue): Unit = {
    val executor = new ScalaExecutor(database)
    scenario("evaluation of " + exp) {
      When("evaluating " + exp)
      Then("it should return " + result)
      assert(executor.execute(Scan("oneRow")) === List(result))
    }
  }

  def checkOperation(opNode: OperatorNode, result: List[MyValue]): Unit = {
    val executor = new ScalaExecutor(database)
    When("evaluating " + opNode)
    Then("it should return " + result)
    assert(executor.execute(opNode) === result)
  }

}

class ExpressionsConst extends ExecutorTest {

  val singleRow = ListValue(List(RecordValue(Map("value" -> IntValue(1)))))
  val database = Map("oneRow" -> singleRow)

  checkExpression(IntConst(1), IntValue(1))
  checkExpression(IntConst(2), IntValue(2))
  checkExpression(BoolConst(true), BooleanValue(true))
  checkExpression(BoolConst(false), BooleanValue(false))
  checkExpression(FloatConst(22.23f), FloatValue(22.23f))
  checkExpression(FloatConst(3.14f), FloatValue(3.14f))
  checkExpression(StringConst("tralala"), StringValue("tralala"))

}

class UnaryExpressions extends ExecutorTest {

  val content = ListValue(List(RecordValue(Map("value" -> IntValue(1))), RecordValue(Map("value" -> IntValue(2)))))
  val database = Map("oneRow" -> content)

  def checkUnaryExp(exp: Exp, results: Map[UnaryOperator, MyValue]): Unit = {
    val f = (op: UnaryOperator, result: MyValue) => checkExpression(UnaryExp(op, exp), result)
    results.foreach { case (op, result) => f(op, result)}
  }

  checkUnaryExp(BoolConst(true), Map(Not() -> BooleanValue(false), ToInt() -> IntValue(1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(1.0f), ToString() -> StringValue("true")))
  checkUnaryExp(BoolConst(false), Map(Not() -> BooleanValue(true), ToInt() -> IntValue(0), ToBool() -> BooleanValue(false), ToFloat() -> FloatValue(0.0f), ToString() -> StringValue("false")))
  checkUnaryExp(IntConst(1), Map(Not() -> BooleanValue(false), Neg() -> IntValue(-1), ToInt() -> IntValue(1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(1.0f), ToString() -> StringValue("1")))
  checkUnaryExp(IntConst(0), Map(Not() -> BooleanValue(true), Neg() -> IntValue(0), ToInt() -> IntValue(0), ToBool() -> BooleanValue(false), ToFloat() -> FloatValue(0.0f), ToString() -> StringValue("0")))
  checkUnaryExp(IntConst(-1), Map(Not() -> BooleanValue(false), Neg() -> IntValue(1), ToInt() -> IntValue(-1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(-1.0f), ToString() -> StringValue("-1")))
  checkUnaryExp(FloatConst(1.1f), Map(Not() -> BooleanValue(false), Neg() -> FloatValue(-1.1f), ToInt() -> IntValue(1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(1.1f), ToString() -> StringValue("1.1")))
  checkUnaryExp(FloatConst(0.0f), Map(Not() -> BooleanValue(true), Neg() -> FloatValue(0.0f), ToInt() -> IntValue(0), ToBool() -> BooleanValue(false), ToFloat() -> FloatValue(0.0f), ToString() -> StringValue("0.0")))
  checkUnaryExp(FloatConst(-1.1f), Map(Not() -> BooleanValue(false), Neg() -> FloatValue(1.1f), ToInt() -> IntValue(-1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(-1.1f), ToString() -> StringValue("-1.1")))
}

class BinaryExpressions extends ExecutorTest {

  val content = ListValue(List(RecordValue(Map("value" -> IntValue(1))), RecordValue(Map("value" -> IntValue(2)))))
  val database = Map("numbers" -> content)

  def checkBinExp(exp1: Exp, exp2: Exp, results: Map[BinaryOperator, MyValue]): Unit = {
    val f = (op: BinaryOperator, result: MyValue) => checkExpression(BinaryExp(op, exp1, exp2), result)
    results.foreach { case (op, result) => f(op, result) }
  }

  val content2 = ListValue(List(RecordValue(Map("name" -> StringValue("one"))), RecordValue(Map("name" -> StringValue("two")))))
  checkBinExp(IntConst(1), IntConst(1), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(IntConst(1), IntConst(2), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(FloatConst(1.2f), FloatConst(1.2f), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(FloatConst(1.1f), FloatConst(1.2f), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(BoolConst(true), BoolConst(true), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(BoolConst(true), BoolConst(false), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(StringConst("tralala"), StringConst("tralala"), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(StringConst("tralala"), StringConst("tralalere"), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
}

class SimpleScan extends  ExecutorTest {
  val singleRow = RecordValue(Map("value" -> IntValue(1)))
  val database = Map("oneRow" -> ListValue(List(singleRow)),
    "twoRows" -> ListValue(List(singleRow, singleRow)))

  checkOperation(Scan("oneRow"), List(singleRow))
  checkOperation(Scan("twoRows"), List(singleRow, singleRow))
}

class SimpleSelect extends SimpleScan {
  checkOperation(Select(List(BoolConst(true)), Scan("oneRow")), List(singleRow))
  checkOperation(Select(List(), Scan("oneRow")), List(singleRow))
  checkOperation(Select(List(BoolConst(false)), Scan("oneRow")), List())
  checkOperation(Select(List(), Scan("twoRows")), List(singleRow, singleRow))
}

class RealQueries extends ExecutorTest {
  val singleRow = RecordValue(Map("value" -> IntValue(1)))
  val database = Map("oneRow" -> ListValue(List(singleRow)),
    "twoRows" -> ListValue(List(singleRow, singleRow)))

  val numberType = RecordType(Seq(AttrType("value", IntType())))
  val numbersType = CollectionType(ListMonoid(), numberType)
  val world = World.newWorld(Map("number" -> numberType, "numbers" -> numbersType), Set(ClassEntity("oneRow", numbersType), ClassEntity("twoRows", numbersType)))
  checkOperation(world.unnest(Driver.parse("""for (d <- oneRow) yield set d.value""")), List(singleRow))
  checkOperation(world.unnest(Driver.parse("""for (d <- twoRows) yield set d.value""")), List(singleRow, singleRow))
  checkOperation(world.unnest(Driver.parse("""for (d <- twoRows, d.value = 20 or d.value = 10) yield set d.value""")), List())
}

class Bug extends ExecutorTest {

  val singleRow = RecordValue(Map("value" -> IntValue(1)))
  val database = Map("oneRow" -> ListValue(List(singleRow)))
  def checkExec(alg: OperatorNode, result: List[Integer]): Unit = {
    scenario("execution of " + alg) {
      When("evaluating " + alg)
      Then("it should return " + result)
      //assert(executor.execute(alg) === result)
    }
  }



  checkExpression(ZeroCollectionMonoid(SetMonoid()), SetValue(Set()))
  checkExpression(ZeroCollectionMonoid(ListMonoid()), ListValue(List()))

  checkExpression(ConsCollectionMonoid(SetMonoid(), IntConst(22)), SetValue(Set(IntValue(22))))
  checkExpression(ConsCollectionMonoid(ListMonoid(), IntConst(22)), ListValue(List(IntValue(22))))

  checkExpression(MergeMonoid(ListMonoid(), ConsCollectionMonoid(ListMonoid(), IntConst(22)), ConsCollectionMonoid(ListMonoid(), FloatConst(23.2f))),
    ListValue(List(IntValue(22), FloatValue(23.2f))))

  checkExpression(RecordCons(Seq(AttrCons("a", IntConst(2)))), RecordValue(Map("a" -> IntValue(2))))
  checkExpression(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), RecordValue(Map("a" -> IntValue(2), "b" -> IntValue(3))))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)))), "a"), IntValue(2))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "a"), IntValue(2))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "b"), IntValue(3))

  // some data: a table of one number

  val numberType = RecordType(Seq(AttrType("value", IntType())))
  val numbersType = CollectionType(ListMonoid(), numberType)
  //val world = World.newWorld(Map("number" -> numberType, "numbers" -> numbersType), Set(ClassEntity("numbers", numbersType)))
  //checkExec(world.unnest(Driver.parse("""for (d <- numbers) yield set d.value""")), new Blocks(List(content)))
}
