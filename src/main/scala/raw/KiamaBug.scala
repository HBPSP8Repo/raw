package raw

object KiamaBug extends App {
  println("Hello world")

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._

  sealed abstract class RootNode extends Product
  sealed abstract class Exp extends RootNode
  case class AttrCons(id: String, e: Exp) extends RootNode
  case class RecordCons(atts: Seq[AttrCons]) extends Exp
  case class BoolConst(v: Boolean) extends Exp

  val a = BoolConst(true)
  println(s"a is $a")
  val b = deepclone(a)
  println(s"b is $b")

  val x = RecordCons(Seq(AttrCons("x", BoolConst(true))))
  println(s"x is $x")
  val y = deepclone(x)
  println(s"y is $y")
}