package raw.executor

import raw._
import algebra.Algebra._

import org.scalatest._
import raw.calculus.SymbolTable.ClassEntity
import raw.calculus.{Driver, World}

/**
 * Created by gaidioz on 1/14/15.
 */

trait PhysicalAlgebraExecutionBehavior {
  this: FlatSpec =>

  def checkExpression(exp: Exp, result: MyValue): Unit = {
    val database: Map[String, List[MyValue]] = Map("oneRow" -> List(RecordValue(Map("value" -> IntValue(1)))))
    val executor = new ScalaExecutor(database)
    it should "evaluate " + exp + " correctly" in {
      assert(executor.execute(Reduce(SetMonoid(), exp, List(), Select(List(), Scan("oneRow")))) === List(SetValue(Set(result))))
    }
  }
}

class PhysicalAlgebraConst extends FlatSpec with PhysicalAlgebraExecutionBehavior {

  "intConst" should behave like checkExpression(IntConst(1), IntValue(1))
}


abstract class ExecutorTest extends FeatureSpec with GivenWhenThen with  Matchers {

  val database: Map[String, List[MyValue]]

  def toString(value: MyValue): String = value match {
    case IntValue(c) => c.toString()
    case BooleanValue(c) => c.toString()
    case FloatValue(c) => c.toString()
    case StringValue(c) => "\"" + c.toString() + "\""
    case SetValue(s) => s.map(toString).mkString("{", ", ", "}")
    case ListValue(s) => s.map(toString).mkString("[", ", ", "]")
    //case RecordValue(s) => s.map((key: String, value: MyValue) => key + "=" + toString(value)).mkString("[", ", ", "]")
    case RecordValue(s) => s.map({v => v._1 + ": " + toString(v._2)}).mkString("[", ", ", "]")
  }

  def checkOperation(opNode: OperatorNode, result: List[MyValue]): Unit = {
    val executor = new ScalaExecutor(database)
    scenario("evaluation of " + opNode) {
      When("evaluating " + opNode)
      Then("it should return " + result)
      assert(executor.execute(opNode) === result)
    }
  }

}

class ExpressionsConst extends ExecutorTest {

  val singleRow = List(RecordValue(Map("value" -> IntValue(1))))
  val database = Map("oneRow" -> singleRow)

  def checkExpression(exp: Exp, result: MyValue): Unit = {
    val executor = new ScalaExecutor(database)
    scenario("evaluation of " + exp) {
      When("evaluating " + exp)
      Then("it should return " + toString(result))
      assert(executor.execute(Reduce(SetMonoid(), exp, List(), Select(List(), Scan("oneRow")))) === List(SetValue(Set(result))))
    }
  }

  checkExpression(IntConst(1), IntValue(1))
  checkExpression(IntConst(2), IntValue(2))
  checkExpression(BoolConst(true), BooleanValue(true))
  checkExpression(BoolConst(false), BooleanValue(false))
  checkExpression(FloatConst(22.23f), FloatValue(22.23f))
  checkExpression(FloatConst(3.14f), FloatValue(3.14f))
  checkExpression(StringConst("tralala"), StringValue("tralala"))

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

  def checkBinExp(exp1: Exp, exp2: Exp, results: Map[BinaryOperator, MyValue]): Unit = {
    val f = (op: BinaryOperator, result: MyValue) => checkExpression(BinaryExp(op, exp1, exp2), result)
    results.foreach { case (op, result) => f(op, result) }
  }

  checkBinExp(IntConst(1), IntConst(1), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(IntConst(1), IntConst(2), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(FloatConst(1.2f), FloatConst(1.2f), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(FloatConst(1.1f), FloatConst(1.2f), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(BoolConst(true), BoolConst(true), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(BoolConst(true), BoolConst(false), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(StringConst("tralala"), StringConst("tralala"), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(StringConst("tralala"), StringConst("tralalere"), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  
  checkExpression(ZeroMonoid(SetMonoid()), SetValue(Set()))
  checkExpression(ZeroMonoid(ListMonoid()), ListValue(List()))
  checkExpression(ConsCollectionMonoid(SetMonoid(), IntConst(22)), SetValue(Set(IntValue(22))))
  checkExpression(ConsCollectionMonoid(ListMonoid(), IntConst(22)), ListValue(List(IntValue(22))))
  checkExpression(MergeMonoid(ListMonoid(), ConsCollectionMonoid(ListMonoid(), IntConst(22)), ConsCollectionMonoid(ListMonoid(), FloatConst(23.2f))), ListValue(List(IntValue(22), FloatValue(23.2f))))

  checkExpression(RecordCons(Seq(AttrCons("a", IntConst(2)))), RecordValue(Map("a" -> IntValue(2))))
  checkExpression(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), RecordValue(Map("a" -> IntValue(2), "b" -> IntValue(3))))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)))), "a"), IntValue(2))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "a"), IntValue(2))
  checkExpression(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "b"), IntValue(3))


}

class SimpleScan extends  ExecutorTest {
  val row = RecordValue(Map("value" -> IntValue(1)))
  val database = Map(
    "oneRow" -> List(row),
    "twoRows" -> List(row, row)
  )
  checkOperation(Scan("oneRow"), List(row))
  checkOperation(Scan("twoRows"), List(row, row))
  checkOperation(Select(List(BoolConst(true)), Scan("oneRow")), List(row))
  checkOperation(Select(List(), Scan("oneRow")), database("oneRow"))
  checkOperation(Select(List(BoolConst(false)), Scan("oneRow")), List())
  checkOperation(Select(List(), Scan("twoRows")), database("twoRows"))
}

class RealQueries extends ExecutorTest {
  val singleRow = RecordValue(Map("value" -> IntValue(1)))
  val database = Map("oneRow" -> List(singleRow),
    "twoRows" -> List(singleRow, singleRow))

  val numberType = RecordType(Seq(AttrType("value", IntType())))
  val numbersType = CollectionType(ListMonoid(), numberType)
  val world = World.newWorld(Map("number" -> numberType, "numbers" -> numbersType), Set(ClassEntity("oneRow", numbersType), ClassEntity("twoRows", numbersType)))

  def checkQuery(query: String, result: MyValue): Unit = {
    val opNode = world.unnest(Driver.parse(query))
    val executor = new ScalaExecutor(database)
    scenario("evaluation of " + query) {
      When("evaluating " + query)
      Then("it should return " + toString(result))
      assert(executor.execute(opNode) === List(result))
    }
  }
  checkQuery("for (d <- oneRow) yield set true", SetValue(Set(BooleanValue(true))))
  checkQuery("for (d <- oneRow) yield list true", ListValue(List(BooleanValue(true))))
  checkQuery("for (d <- twoRows) yield set true", SetValue(Set(BooleanValue(true))))
  checkQuery("for (d <- twoRows) yield list true", ListValue(List(BooleanValue(true), BooleanValue(true))))
  checkQuery("for (d <- twoRows) yield list d.value", ListValue(List(IntValue(1), IntValue(1))))
  checkQuery("for (d <- twoRows) yield set d.value", SetValue(Set(IntValue(1))))
}
