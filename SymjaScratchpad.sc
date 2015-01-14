import org.matheclipse.core.eval.EvalUtilities
import org.matheclipse.core.expression._

object SymjaScratchpad {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  //val x = F.$s("a")
  //val test = F.D(F.Times(F.Sin(x), F.Cos(x)), x)
  
  val a = F.$s("a")                               //> Config.SHOW_STACKTRACE == true
                                                  //| a  : org.matheclipse.core.interfaces.ISymbol = a
  val b = F.$s("b")                               //> b  : org.matheclipse.core.interfaces.ISymbol = b
  val test = F.Equal(F.Plus(a, b), a)             //> test  : org.matheclipse.core.interfaces.IAST = b+a==a
  
  val util = new EvalUtilities()                  //> util  : org.matheclipse.core.eval.EvalUtilities = org.matheclipse.core.eval.
                                                  //| EvalUtilities@1019b028
  util.evaluate(test)                             //> res0: org.matheclipse.core.interfaces.IExpr = b+a==a
   
	util.evaluate("2 * a + b - (a) < 0")      //> res1: org.matheclipse.core.interfaces.IExpr = b+a<0
   
  util.evaluate("a && (b || c)")                  //> res2: org.matheclipse.core.interfaces.IExpr = a&&(b||c)

}