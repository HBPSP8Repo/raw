package raw.executor

import raw._
import algebra.Algebra._

import org.scalatest.{FeatureSpec, GivenWhenThen, FunSpec, FunSuite}

/**
 * Created by gaidioz on 1/14/15.
 */
class ExecutorFun extends FeatureSpec with GivenWhenThen {

  val content = ListValue(List(RecordValue(Map("value" -> IntValue(1))), RecordValue(Map("value" -> IntValue(2)))))
  val content2 = ListValue(List(RecordValue(Map("name" -> StringValue("one"))), RecordValue(Map("name" -> StringValue("two")))))
  val executor = new scala(Map("numbers" -> content))

  def check(exp: Exp, result: MyValue): Unit = {
    scenario("evaluation of " + exp) {
      When("evaluating " + exp)
      Then("it should return " + result)
      assert(executor.expEval(exp, Map()) === result)
    }
  }

  def checkUnaryExp(exp: Exp, results: Map[UnaryOperator, MyValue]): Unit = {
    val f = (op: UnaryOperator, result: MyValue) => check(UnaryExp(op, exp), result)
    results.foreach { case (op, result) => f(op, result) }
  }

  def checkBinExp(exp1: Exp, exp2: Exp, results: Map[BinaryOperator, MyValue]): Unit = {
    val f = (op: BinaryOperator, result: MyValue) => check(BinaryExp(op, exp1, exp2), result)
    results.foreach { case (op, result) => f(op, result) }
  }

  def checkExec(alg: OperatorNode, result: Blocks): Unit = {
    scenario("execution of " + alg) {
      When("evaluating " + alg)
      Then("it should return " + result)
      assert(executor.execute(alg) === result)
    }
  }

  check(IntConst(1), IntValue(1))
  check(IntConst(2), IntValue(2))
  check(BoolConst(true), BooleanValue(true))
  check(BoolConst(false), BooleanValue(false))
  check(FloatConst(22.23f), FloatValue(22.23f))
  check(FloatConst(3.14f), FloatValue(3.14f))
  check(StringConst("tralala"), StringValue("tralala"))

  checkBinExp(IntConst(1), IntConst(1), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(IntConst(1), IntConst(2), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(FloatConst(1.2f), FloatConst(1.2f), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(FloatConst(1.1f), FloatConst(1.2f), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(BoolConst(true), BoolConst(true), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(BoolConst(true), BoolConst(false), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))
  checkBinExp(StringConst("tralala"), StringConst("tralala"), Map(Eq() -> BooleanValue(true), Neq() -> BooleanValue(false)))
  checkBinExp(StringConst("tralala"), StringConst("tralalere"), Map(Eq() -> BooleanValue(false), Neq() -> BooleanValue(true)))

  checkUnaryExp(BoolConst(true), Map(Not() -> BooleanValue(false), ToInt() -> IntValue(1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(1.0f), ToString() -> StringValue("true")))
  checkUnaryExp(BoolConst(false), Map(Not() -> BooleanValue(true), ToInt() -> IntValue(0), ToBool() -> BooleanValue(false), ToFloat() -> FloatValue(0.0f), ToString() -> StringValue("false")))
  checkUnaryExp(IntConst(1), Map(Not() -> BooleanValue(false), Neg() -> IntValue(-1), ToInt() -> IntValue(1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(1.0f), ToString() -> StringValue("1")))
  checkUnaryExp(IntConst(0), Map(Not() -> BooleanValue(true), Neg() -> IntValue(0), ToInt() -> IntValue(0), ToBool() -> BooleanValue(false), ToFloat() -> FloatValue(0.0f), ToString() -> StringValue("0")))
  checkUnaryExp(IntConst(-1), Map(Not() -> BooleanValue(false), Neg() -> IntValue(1), ToInt() -> IntValue(-1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(-1.0f), ToString() -> StringValue("-1")))
  checkUnaryExp(FloatConst(1.1f), Map(Not() -> BooleanValue(false), Neg() -> FloatValue(-1.1f), ToInt() -> IntValue(1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(1.1f), ToString() -> StringValue("1.1")))
  checkUnaryExp(FloatConst(0.0f), Map(Not() -> BooleanValue(true), Neg() -> FloatValue(0.0f), ToInt() -> IntValue(0), ToBool() -> BooleanValue(false), ToFloat() -> FloatValue(0.0f), ToString() -> StringValue("0.0")))
  checkUnaryExp(FloatConst(-1.1f), Map(Not() -> BooleanValue(false), Neg() -> FloatValue(1.1f), ToInt() -> IntValue(-1), ToBool() -> BooleanValue(true), ToFloat() -> FloatValue(-1.1f), ToString() -> StringValue("-1.1")))

  check(ZeroCollectionMonoid(SetMonoid()), SetValue(Set()))
  check(ZeroCollectionMonoid(ListMonoid()), ListValue(List()))

  check(ConsCollectionMonoid(SetMonoid(), IntConst(22)), SetValue(Set(IntValue(22))))
  check(ConsCollectionMonoid(ListMonoid(), IntConst(22)), ListValue(List(IntValue(22))))

  check(MergeMonoid(ListMonoid(), ConsCollectionMonoid(ListMonoid(), IntConst(22)), ConsCollectionMonoid(ListMonoid(), FloatConst(23.2f))),
        ListValue(List(IntValue(22), FloatValue(23.2f))))

  check(RecordCons(Seq(AttrCons("a", IntConst(2)))), RecordValue(Map("a" -> IntValue(2))))
  check(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), RecordValue(Map("a" -> IntValue(2), "b" -> IntValue(3))))
  check(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)))), "a"), IntValue(2))
  check(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "a"), IntValue(2))
  check(RecordProj(RecordCons(Seq(AttrCons("a", IntConst(2)), AttrCons("b", IntConst(3)))), "b"), IntValue(3))

  // some data: a table of one number



  val x = new Blocks(List(content, content))
  x.foreach(println)
  checkExec(Scan("numbers"), new Blocks(List(content)))
  /*
  checkExec(Select(List(), Scan("numbers")), content)
  checkExec(Select(List(BoolConst(true)), Scan("numbers")), content)
  checkExec(Select(List(BoolConst(false)), Scan("numbers")), ListValue(List()))
  checkExec(Select(List(BinaryExp(Eq(), IntConst(1), IntConst(1))), Scan("numbers")), content)
  checkExec(Select(List(BinaryExp(Eq(), IntConst(1), IntConst(2))), Scan("numbers")), ListValue(List()))
  */

  val numberType = RecordType(Seq(AttrType("value", IntType())))
  val numbersType = CollectionType(ListMonoid(), numberType)
  //val world = World.newWorld(Map("number" -> numberType, "numbers" -> numbersType), Set(ClassEntity("numbers", numbersType)))
  //checkExec(world.unnest(Driver.parse("""for (d <- numbers) yield set d.value""")), new Blocks(List(content)))
}