package raw.experiments

object Existentials {
//  def foo(a:Array[T] forSome {type T <: CharSequence}) = {
//    println("Size: " + a.length)
//    println("First char: " + a(0).charAt(0))
//  }

  def foo(a:Array[T] forSome {type T <: { def charAt(index: Int):Char} }) = {
    println("Size: " + a.length)
    println("First char: " + a(0).charAt(0))
  }

  def main(args: Array[String]) {
    foo(Array("a", "b"))
//    foo(Array(1, 2))
  }
}
