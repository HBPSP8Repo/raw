package raw
package executor.spark

object Codegen {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  import scala.tools.reflect.ToolBox

  protected val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()


  def buildTuple(atts: Seq[AttrType]): (String => Map[String, Any]) = {
    val map = "Map(" + atts.view.zipWithIndex.map{ case (att, i) => s""""${att.idn}" -> """ + (att.tipe match {
      case IntType()    => s"cols($i).toInt"
      case FloatType()  => s"cols($i).toFloat"
      case BoolType()   => s"cols($i).toBoolean"
      case StringType() => s"cols($i)"
      case t            => throw new RuntimeException(s"Type $t not supported in text/csv files")
    })}.mkString(",") + ")"
    val textCode = s"""
    (line: String) => {
      val cols = line.split(",")
      List($map)
    }
    """
    println(textCode)
    val code = toolbox.parse(textCode)
    toolbox.eval(code).asInstanceOf[(String => Map[String, Any])]
  }


  def buildPredicate(ps: List[algebra.Exp]): (Any => Boolean) = {
    val predicate1 = algebra.BinaryExp(Eq(), algebra.RecordProj(algebra.Arg(0), "office"), algebra.StringConst("BC100"))

    def apply(e: algebra.Exp): String = e match {
      case algebra.BinaryExp(_: Eq, lhs, rhs) => s"${apply(lhs)} == ${apply(rhs)}"
      case algebra.RecordProj(e, idn) => s"""${apply(e)}("$idn")"""
      case algebra.Arg(idn) => "p"
      case algebra.StringConst(v) => s""""$v""""
    }


    val textCode = s"""(p: Map[String, Any]) => ${apply(predicate1)}"""
    //val textCode = s"""() => { ((p: Map[String, Any]) => (${apply(predicate1)})) }"""
    val code = toolbox.parse(textCode)
    val f = toolbox.eval(code).asInstanceOf[(Any => Boolean)]
    println("**** " + toolbox.eval(code))
    f
    //val f = toolbox.eval(code).asInstanceOf[(() => (Any => Boolean))]
    //p => { f(p) }

    //p => {val np = p.asInstanceOf[Map[String, Any]]; val np1 = np.get("office") match { case Some(b) => b.toString() case None => "" }; val code = q"""$np1 == "BC100""""; toolbox.compile(code)().asInstanceOf[Boolean] }
  }
}
